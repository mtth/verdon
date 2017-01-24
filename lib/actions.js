/* jshint esversion: 6, node: true */

'use strict';

// TODO: Expand info to be able to return any message's or type's schema.

const proxy = require('./proxy');

const avro = require('avsc');
const http = require('http');
const net = require('net');
const path = require('path');
const request = require('request');
const stream = require('stream');
const util = require('util');
const {parse: parseUrl} = require('url');

const debug = util.debuglog('verdon');


const DEFAULT_PORT = 24950;

const CONTENT_TYPE = 'avro/binary';

// Actions.

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
    if (jsonReq === undefined) {
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

function info(url, opts, cb) {
  const urlObj = parseUrl(url);
  if (urlObj.protocol === 'file:') {
    avro.assembleProtocol(urlObj.path, function (err, ptcl) {
      let svc;
      try {
        svc = avro.Service.forProtocol(ptcl);
      } catch (cause) {
        cb(cause);
        return;
      }
      done(null, svc);
    });
  } else {
    createClient(url, opts, function (err, client) {
      if (err) {
        cb(err);
        return;
      }
      client.destroyChannels();
      done(null, client.service);
    });
  }

  function done(err, svc) {
    if (err) {
      cb(err);
      return;
    }

    const ptcl = svc.protocol;
    const msgName = opts.message;
    const typeName = opts.type;
    if (msgName) {
      const msg = svc.message(msgName);
      if (!msg) {
        cb(new Error(`no such message: ${msgName}`));
        return;
      }
      if (opts.json) {
        cb(null, JSON.stringify(msg.schema({exportAttrs: true})));
      } else {
        cb(null, msg.doc);
      }
    } else if (typeName) {
      const type = svc.type(typeName);
      if (!type) {
        cb(new Error(`no such type: ${opts.type}`));
        return;
      }
      if (opts.json) {
        cb(null, JSON.stringify(type.schema({exportAttrs: true})));
      } else {
        cb(null, type.doc);
      }
    } else if (opts.json) {
      cb(null, JSON.stringify(ptcl));
    } else {
      cb(null, svc.doc);
    }
  }
}

function startProxy(fpath, opts, cb) {
  const serverOrProxy = require(path.join(fpath));
  const httpProxy =  serverOrProxy instanceof proxy.HttpProxy ?
    serverOrProxy :
    proxy.HttpProxy.forServer(serverOrProxy);
  httpProxy.server.listen(opts.port || DEFAULT_PORT, cb);
}

// Utilities.

/** Generate the transport corresponding to a given URL. */
function createTransport(url, cb) {
  debug('creating transport for url: %s', url);
  const urlObj = parseUrl(url);
  switch (urlObj.protocol) {
    case 'http:':
      debug('using stateless HTTP transport');
      cb(null, function (cb) {
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
            headers: {'content-type': CONTENT_TYPE}
          }).on('error', cb)
            .on('response', (res) => {
              debug('got response');
              cb(null, res);
            });
        });
      });
      break;
    case 'tcp:':
      debug('using stateful TCP transport');
      if (urlObj.path && !urlObj.port && !urlObj.hostname) {
        cb(null, net.createConnection({path: urlObj.path}));
      } else if (!urlObj.path) {
        cb(null, net.createConnection({
          port: urlObj.port || DEFAULT_PORT,
          host: urlObj.hostname
        }));
      } else {
        unsupportedUrl();
      }
      break;
    case 'file:':
      debug('using stateful in-memory transport');
      if (!urlObj.port && !urlObj.hostname) {
        debug(`path: ${urlObj.path}`);
        const createServer = require(path.join(urlObj.path));
        createServer(function (err, server) {
          if (err) {
            cb(err);
            return;
          }
          const opts = {objectMode: true};
          const transports = [
            new stream.PassThrough(opts),
            new stream.PassThrough(opts)
          ];
          server.createChannel({
            readable: transports[0],
            writable: transports[1]
          });
          cb(null, {readable: transports[1], writable: transports[0]});
        });
      } else {
        unsupportedUrl();
      }
      break;
    default:
      unsupportedUrl();
  }

  function unsupportedUrl() {
    cb(new Error(`unsupported URL: ${url}`));
  }
}

/** Generate an appropriate client for the given URL. */
function createClient(url, opts, cb) {
  createTransport(url, function (err, transport) {
    if (err) {
      cb(err);
      return;
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
  });
}

/** Start a server for the given URL. */
function listen(url, server, opts, cb) {
  debug('using URL: %s', url);
  const urlObj = parseUrl(url);
  switch (urlObj.protocol) {
    case 'http:':
      const httpServer = http.createServer()
        .on('request', function (req, res) {
          if (
            req.method !== 'POST' ||
            req.headers['content-type'] !== CONTENT_TYPE
          ) {
            res.statusCode = 400;
            res.end();
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
        .listen(urlObj.port || 80, urlObj.hostname);
      cb(null, httpServer);
      break;
    case 'tcp:':
      debug('using stateful transport');
      const tcpServer = net.createServer()
        .on('connection', function (con) {
          server.createChannel(con, {scope: opts.scope});
        });
      if (urlObj.path && !urlObj.port && !urlObj.hostname) {
        tcpServer.listen({path: urlObj.path});
      } else if (!urlObj.path) {
        tcpServer.listen({
          port: urlObj.port || DEFAULT_PORT,
          host: urlObj.hostname
        });
      }
      cb(null, tcpServer);
      break;
    default:
      cb(new Error(`unsupported URL: ${url}`));
  }
}


module.exports = {
  call,
  info,
  listen,
  startProxy,
  // For testing:
  _createClient: createClient,
  _createTransport: createTransport
};
