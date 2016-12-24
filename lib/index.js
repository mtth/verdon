/* jshint esversion: 6, node: true */

'use strict';

const avro = require('avsc');
const http = require('http');
const net = require('net');
const stream = require('stream');
const request = require('request');
const util = require('util');
const {parse: parseUrl} = require('url');

const debug = util.debuglog('verdon');

// Actions.

function call(url, msgName, jsonReq, opts, cb) {
  createClient(url, function (err, client) {
    const msg = client.getService().getMessage(msgName);
    if (!msg) {
      cb(new Error(`no such message: ${msgName}`));
      return;
    }
    if (!jsonReq) {
      cb(new Error('TODO'));
      return;
    }
    let req;
    try {
      req = msg.getRequestType().fromString(jsonReq);
    } catch (err) {
      cb(err);
      return;
    }
    client.emitMessage(msg.getName(), req, (err, res) => {
      if (err) {
        cb(err);
        return;
      }
      if (!msg.isOneWay()) {
        cb(null, msg.getResponseType().toString(res));
      }
      client.destroyEmitters();
    });
  });
}

function info(url, msgName, opts, cb) {
  createClient(url, (err, client) => {
    if (err) {
      cb(err);
      return;
    }
    var svc = client.getService();
    var ptcl = svc.getProtocol();
    if (msgName && opts.json) {
      const msg = svc.getMessage(msgName);
      if (!msg) {
        cb(new Error(`no such message: ${msgName}`));
        return;
      }
      if (opts.json) {
        cb(null, JSON.stringify(ptcl.messages[msgName]));
      } else {
        cb(null, msg.getDocumentation());
      }
    } else if (opts.json) {
      cb(null, JSON.stringify(ptcl));
    } else {
      cb(null, svc.getDocumentation());
    }
  });
}

function serve(url, ptclPath, opts, cb) {
  avro.assembleProtocol(ptclPath, function (err, ptcl) {
    if (err) {
      cb(err);
      return;
    }

    const svc = avro.Service.forProtocol(ptcl, {wrapUnions: true});

    const server = svc.createServer();

    for (let msg of svc.getMessages()) {
      server.onMessage(msg.getName(), createMessageHandler(msg));
    }

    const obj = parseUrl(url);
    debug('using url: %s', url);
    switch (obj.protocol) {
      case 'http:':
        http.createServer()
          .on('request', function (req, res) {
            if (req.method !== 'POST') {
              // TODO: Check content type.
              return;
            }
            server.createListener(
              function (cb) {
                cb(null, res);
                return req;
              },
              {scope: opts.scope}
            );
          })
          .listen(obj.port, obj.hostname);
        break;
      case 'tcp:':
        debug('using stateful transport');
        net.createServer()
          .on('connection', function (con) {
            server.createListener(con, {scope: opts.scope});
          })
          .listen({port: obj.port, host: obj.hostname});
        break;
      // case 'file:': TODO: UNIX ocket.
      default:
        cb(new Error(`unsupported url: ${url}`));
    }
  });
}

// Utilities.

/** Generate an appropriate client for the given URL. */
function createClient(url, opts, cb) {
  if (!cb && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }
  opts = opts || {};

  const transport = (function () {
    const obj = parseUrl(url);
    debug('using url: %s', url);
    switch (obj.protocol) {
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
          port: obj.port,
          host: obj.hostname
        });
      // case 'file:' TODO: Use (stateful) UNIX socket.
      default:
        return undefined;
    }
  })();
  if (!transport) {
    cb(new Error(`unsupported url: ${url}`));
    return;
  }

  const isStateless = typeof transport == 'function';
  if (opts.protocol) {
    debug('assembling protocol');
    // TODO: Support client protocol.
    cb(new Error('not yet supported'));
    return;
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
    const client = svc.createClient();
    client.createEmitter(transport, {noPing: isStateless, scope: opts.scope});
    cb(null, client);
  }
}

function createMessageHandler(msg) {
  return function (req, cb) {
    cb(null, msg.getResponseType().random());
  };
}

module.exports = {
  call,
  info,
  serve
};
