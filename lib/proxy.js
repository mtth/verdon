/* jshint esversion: 6, node: true */

// TODO: Allow support for late server bindinging. Or some way of indicating
// which servers are ready and which aren't. Related to the proxy's status: OK
// only when all servers have been bindinged. This will be done as follows:
// Servers will be registered separately (likely on an "envelope" TBD) and this
// envelope will be started with a list of services that it should expose. The
// status will be OK when all these services' servers have been registered
// (registration also bindings them).

'use strict';

/** Infrastructure. */

const avro = require('avsc');
const events = require('events');
const http = require('http');
const stream = require('stream');
const {parse: parseUrl} = require('url');
const util = require('util');
const ws = require('ws');
const wsStream = require('websocket-stream');

const debug = util.debuglog('verdon:proxy');

const HEADERS_TYPE = avro.Type.forSchema({type: 'map', values: 'bytes'});

/** HTTP service proxy. */
class HttpProxy extends events.EventEmitter {

  constructor(receiver) {
    super();
    this._bindings = new Map();
    this._receiver = receiver;
  }

  bindClientProvider(clientProvider, {scope = ''} = {}) {
    this._bindings.set(scope, {clientProvider, scope});
    return this;
  }

  bindServer(server, {scope = ''} = {}) {
    this._bindings.set(scope, {scope, server});
    this.emit('server', server, scope);
    return this;
  }

  createChannels(transport, scopes, {objectMode = false} = {}) {
    const bindings = [];
    for (const scope of scopes) {
      const binding = this._bindings.get(scope);
      if (!binding || !binding.server && typeof transport == 'function') {
        return new Error(`invalid scope: ${scope}`);
      }
      bindings.push(binding);
    }
    for (const binding of bindings) {
      debug('creating channel with scope %s', binding.scope);
      const channel = (binding.server || binding.clientProvider())
        .createChannel(transport, {objectMode, scope: binding.scope});
      this.emit('channel', channel);
    }
  }

  /** Generic request handler. */
  requestHandler(opts, cb) {
    if (!cb && typeof opts == 'function') {
      cb = opts;
      opts = undefined;
    }
    return (req, res) => {
      res.setHeader('Content-Type', 'text/plain');
      this._receiver(req.headers, (err) => {
        if (err) {
          res.writeHead(403);
          res.end(err.message);
          return;
        }
        cb(req, res, function (err) {
          if (err) {
            res.setHeader('Content-Type', 'text/plain');
            res.writeHead(400);
            res.end(err.message);
          }
        });
      });
    };
  }

  postRequestHandler(prefix = '/') {
    prefix = prefix.replace(/\/+$/, '') + '/';
    return this.requestHandler((req, res, cb) => {
      if (req.method !== 'POST' || !req.url.startsWith(prefix)) {
        return; // Ignore this request.
      }
      const scopes = extractScopes(req.url.substr(prefix.length));
      const contentType = req.headers['content-type'];
      if (contentType === 'avro/binary') {
        res.setHeader('Content-Type', contentType);
        cb(this.createChannels(
          function (fn) { fn(null, res); return req; }, scopes));
      } else if (contentType === 'avro/json') {
        if (scopes.length !== 1) {
          cb(new Error('invalid scopes'));
          return;
        }
        const binding = this._bindings.get(scopes[0]);
        if (!binding || !binding.server) {
          cb(new Error('invalid scope'));
          return;
        }
        const server = binding.server;
        parseBody(req, server.service, (err, jreq) => {
          if (err) {
            cb(err);
            return;
          }
          const streams = [new stream.PassThrough(), new stream.PassThrough()];
          const client = server.service.createClient({
              buffering: true,
              strictTypes: true
            });
          client.createChannel(
            {readable: streams[0], writable: streams[1]}, {scope: scopes[0]});
          const jres = {headers: {}};
          client
            .use(function (wreq, wres, next) {
              if (jreq.headers) {
                wreq.headers = jreq.headers;
              }
              next(null, function (err, prev) {
                if (err) {
                  prev(err);
                  return;
                }
                if (wres) {
                  jres.headers = JSON.parse(
                    HEADERS_TYPE.toString(wres.headers));
                }
                prev();
              });
            })
            .emitMessage(jreq.message, jreq.request, function (err, res_) {
              client.destroyChannels();
              const msg = this.message;
              res.setHeader('Content-Type', 'avro/json');
              if (err !== undefined) {
                jres.error = JSON.parse(msg.errorType.toString(err));
              } else {
                jres.response = JSON.parse(msg.responseType.toString(res_));
              }
              res.end(JSON.stringify(jres));
            });
          cb(this.createChannels(
            {readable: streams[1], writable: streams[0]}, scopes));
        });
      } else {
        cb(new Error(`invalid content type: ${contentType}`));
      }
    });
  }

  /** Generic upgrade handler. */
  upgradeHandler(cb) {
    return (req, sock, head) => {
      this._receiver(req.headers, (err) => {
        if (err) {
          socketError(sock, '403 Forbidden', err.message);
          return;
        }
        cb(req, sock, head, function (err) {
          if (err) {
            socketError(sock, '400 Bad Request', err.message);
          }
        });
      });
    };
  }

  connectHandler() {
    return this.upgradeHandler((req, sock, head, cb) => {
      if (head.length) {
        cb(new Error('unsupported trailing data'));
        return;
      }
      sock.write('HTTP/1.1 200 Connection Established\r\n\r\n');
      cb(this.createChannels(sock, extractScopes(req.url)));
    });
  }

  webSocketHandler(opts) {
    const wsServer = new ws.Server({noServer: true});
    return this.upgradeHandler(function (req, sock, head, cb) {
      wsServer.handleUpgrade(req, sock, head, function (client) {
        cb(this.createChannels(
          wsStream(client, opts), extractScopes(req.url)));
      });
    });
  }
}

/** Proxy creation entry point. */
function createProxy(opts, receiver) {
  if (!receiver && typeof opts == 'function') {
    receiver = opts;
    opts = undefined;
  }
  // Accept everything by default.
  receiver = receiver || function (hdrs, cb) { cb(); };
  return new HttpProxy(receiver, opts);
}

/** Tunnel creation entry point (for clients). */
function startTunnel(url, opts, cb) {
  if (!cb && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }
  const obj = parseUrl(url);
  opts = opts || {};
  http.request({
    path: obj.path,
    host: obj.hostname,
    port: obj.port,
    headers: opts.headers,
    method: 'CONNECT'
  }).on('connect', function (res, sock, head) {
    if (res.statusCode !== 200 || head.length) {
      // We delay the callback until we have read the entire response's body
      // (otherwise we might not be able to retrieve the correct error).
      const bufs = [head];
      sock
        .on('data', function (buf) { bufs.push(buf); })
        .on('end', function () {
          cb(new Error(Buffer.concat(bufs).toString() || res.statusMessage));
        })
        .end();
      return;
    }
    cb(null, sock);
  }).on('error', cb)
    .end();
}

// Helpers.

/** Send an error message and terminate a connection. */
function socketError(sock, status, msg) {
  sock.write(`HTTP/1.1 ${status}\r\n`);
  sock.write('Content-Type: text/plain\r\n\r\n');
  sock.end(msg);
}

/** Parse a request's JSON body. */
function parseBody(req, service, cb) {
  const bufs = [];
  req
    .on('error', cb)
    .on('data', function (buf) { bufs.push(buf); })
    .on('end', function () {
      const str = Buffer.concat(bufs).toString();
      let obj;
      try {
        obj = JSON.parse(str);
      } catch (err) {
        cb(err);
        return;
      }
      const msg = service.message(obj.message);
      if (!msg) {
        cb(new Error(`unknown message: ${obj.message}`));
        return;
      }
      if (obj.headers) {
        try {
          obj.headers = HEADERS_TYPE.fromString(JSON.stringify(obj.headers));
        } catch (err) {
          // Do nothing, continue further to allow for a better error message.
        }
      } else {
        obj.headers = {};
      }
      try {
        obj.request = msg.requestType.fromString(JSON.stringify(obj.request));
      } catch (err) {
        // Idem.
      }
      cb(null, obj);
    });
}

/** URL parsing logic. */
function extractScopes(url) {
  const segments = url.split('/');
  return segments[0].split('+');
}


module.exports = {
  createProxy,
  startTunnel
};
