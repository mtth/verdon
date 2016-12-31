/* jshint esversion: 6, node: true */

'use strict';

// TODO: Expand info to be able to return any message's or type's schema.

const utils = require('./utils');
const avro = require('avsc');
const chalk = require('chalk');
const http = require('http');
const net = require('net');
const request = require('request');
const stream = require('stream');
const util = require('util');
const {parse: parseUrl} = require('url');

const debug = util.debuglog('verdon');

// Actions.

function assemble(path, typeName, opts, cb) {
  avro.assembleProtocol(path, opts, function (err, ptcl) {
    if (err) {
      cb(err);
      return;
    }
    let svc;
    try {
      // Always build for error checking.
      svc = avro.Service.forProtocol(ptcl, {wrapUnions: true});
    } catch (cause) {
      cb(cause);
      return;
    }
    if (typeName) {
      const type = svc.type(typeName);
      if (!type) {
        cb(new Error(`no such type: ${typeName}`));
        return;
      }
      cb(null, JSON.stringify(type.getSchema({exportAttrs: true})));
    } else {
      cb(null, JSON.stringify(svc.protocol));
    }
  });
}

function call(url, msgName, jsonReq, opts, cb) {
  createClient(url, opts, function (err, client) {
    if (err) {
      cb(err);
      return;
    }

    const svc = client.service;

    // Header handling.
    const hdrs = (opts.header || []).map(function (obj) {
      const type = svc.type(obj.name) || avro.Type.forSchema(obj.name);
      const val = type.fromString(obj.val);
      return {key: obj.key, val: type.toBuffer(val)};
    });
    client.use(function (wreq, wres, next) {
      for (const hdr of hdrs) {
        const key = hdr.key;
        const val = hdr.val;
        debug('header > %s=%j', key, val.toString('binary'));
        wreq.headers[key] = val;
      }
      next(null, function (err, prev) {
        const hdr = wres.headers;
        for (const key of Object.keys(hdr)) {
          const val = hdr[key];
          debug('header < %s=%j', key, val.toString('binary'));
        }
        prev(err);
      });
    });

    const msg = svc.message(msgName);
    if (!msg) {
      cb(new Error(`no such message: ${msgName}`));
      return;
    }
    if (!jsonReq) {
      debug('reading request from stdin');
      const bufs = [];
      process.stdin
        .on('data', function (buf) { bufs.push(buf); })
        .on('end', function () {
          jsonReq = Buffer.concat(bufs).toString();
          done();
        });
    } else {
      process.nextTick(done);
    }

    function done() {
      let req;
      try {
        req = msg.requestType.fromString(jsonReq);
      } catch (err) {
        cb(err);
        return;
      }
      debug('emitting message');
      client.emitMessage(msg.name, req, (err, res) => {
        if (err) {
          cb(err);
          return;
        }
        if (!msg.oneWay) {
          cb(null, msg.responseType.toString(res));
        }
        client.destroyChannels();
      });
    }
  });
}

function info(url, msgName, opts, cb) {
  createClient(url, opts, (err, client) => {
    if (err) {
      cb(err);
      return;
    }
    var svc = client.service;
    var ptcl = svc.protocol;
    if (msgName && opts.json) {
      const msg = svc.message(msgName);
      if (!msg) {
        done(new Error(`no such message: ${msgName}`));
        return;
      }
      if (opts.json) {
        done(null, JSON.stringify(ptcl.messages[msgName]));
      } else {
        done(null, msg.doc);
      }
    } else if (opts.json) {
      done(null, JSON.stringify(ptcl));
    } else {
      let doc = `\n${chalk.blue(svc.name)}\n\n`;
      if (svc.doc) {
        doc += utils.indent(svc.doc, 2);
      }
      const msgs = svc.messages.slice().sort(function (msg1, msg2) {
        return msg1.name.localeCompare(msg2.name);
      });
      if (msgs.length) {
        doc += '\n\nMessages:\n\n';
        for (let msg of msgs) {
          doc += utils.indent(signature(msg), 2) + '\n';
          if (msg.doc) {
            doc += utils.indent(msg.doc, 4);
            doc += '\n';
          }
          doc += '\n';
        }
      }
      done(null, doc);
    }

    function done(err, str) {
      client.destroyChannels();
      cb(err, str);
    }
  });
}

function signature(msg) {
  const opts = {noDeref: true};
  let schema = msg.isOneWay() ? 'void' : msg.responseType.getSchema(opts);
  if (Array.isArray(schema)) {
    // Union.
    schema = '' + schema.join();
  } else if (typeof schema != 'string') {
    // Anonymous record.
    schema = 'record';
  }
  let str = chalk.yellow(schema);
  str += ' ' + chalk.blue(msg.name) + '(';
  let args = msg.requestType.getFields().map(function (field) {
    return chalk.yellow(field.getType().getSchema(opts)) + ' ' + field.getName();
  });
  str += args.join(', ') + ')';
  return str;
}

function serve(url, ptclPath, opts, cb) {
  avro.assembleProtocol(ptclPath, function (err, ptcl) {
    if (err) {
      cb(err);
      return;
    }

    const svc = avro.Service.forProtocol(ptcl, {wrapUnions: true});
    const server = svc.createServer();
    for (let msg of svc.messages) {
      // Attach a handler returning random responses to each.
      server.onMessage(msg.name, createMessageHandler(msg));
    }

    const urlObj = parseUrl(url);
    debug('using url: %s', url);
    switch (urlObj.protocol) {
      case 'http:':
        http.createServer()
          .on('request', function (req, res) {
            if (req.method !== 'POST') {
              // TODO: Check content type.
              return;
            }
            server.createChannel(
              function (cb) {
                cb(null, res);
                return req;
              },
              {scope: opts.scope}
            );
          })
          .listen(urlObj.port, urlObj.hostname);
        break;
      case 'tcp:':
        debug('using stateful transport');
        net.createServer()
          .on('connection', function (con) {
            server.createChannel(con, {scope: opts.scope});
          })
          .listen({port: urlObj.port, host: urlObj.hostname});
        break;
      // case 'file:': TODO: UNIX ocket.
      default:
        cb(new Error(`unsupported url: ${url}`));
    }
  });
}

// Utilities.

/** Generate the transport corresponding to a given URL. */
function createTransport(url) {
  debug('creating transport for url: %s', url);
  const urlObj = parseUrl(url);
  switch (urlObj.protocol) {
    case 'http:':
    case 'https:':
      debug('using stateless transport');
      return (cb) => {
        // Some servers don't accept streaming requests, since we aren't too
        // concerned by having to support very large requests we play it safe
        // and buffer the entire request in memory and sent it in once go.
        const bufs = [];
        return new stream.Writable({
          write: (buf, encoding, cb) => {
            bufs.push(buf);
            cb();
          }
        }).on('finish', () => {
          const body = Buffer.concat(bufs);
          debug('sending request (size: %d)', body.length);
          request.post({
            url: url,
            encoding: null, // Expect binary data.
            followAllRedirects: true,
            body: body,
            headers: {'content-type': 'avro/binary'}
          }).on('error', cb)
            .on('response', (res) => {
              debug('got response');
              cb(null, res);
            });
        });
      };
    case 'tcp:':
      debug('using stateful transport');
      return net.createConnection({
        port: urlObj.port,
        host: urlObj.hostname
      });
    // case 'file:' TODO: Use (stateful) UNIX socket.
    default:
      throw new Error(`unsupported url: ${url}`);
  }
}

/** Generate an appropriate client for the given URL. */
function createClient(transport, opts, cb) {
  if (typeof transport == 'string') {
    try {
      transport = createTransport(transport, opts);
    } catch (cause) {
      cb(cause);
      return;
    }
  }

  const isStateless = typeof transport == 'function';
  if (opts.protocol) {
    debug('assembling protocol');
    avro.assembleProtocol(opts.protocol, {importHook: opts.importHook}, done);
  } else {
    debug('discovering protocol');
    avro.discoverProtocol(transport, {scope: opts.scope}, done);
  }

  function done(err, ptcl) {
    if (err) {
      cb(err);
      return;
    }
    debug('instantiating service');
    const svc = avro.Service.forProtocol(ptcl, {wrapUnions: true});
    const client = svc.createClient({strictErrors: true});
    client.createChannel(transport, {noPing: isStateless, scope: opts.scope});
    cb(null, client);
  }
}

/** Generate a placeholder server handler. */
function createMessageHandler(msg) {
  return function (req, cb) {
    cb(null, msg.responseType.random());
  };
}

module.exports = {
  assemble,
  call,
  info,
  serve,
  createClient,
  createTransport
};
