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

  bind(server, {scope = ''} = {}) {
    this._bindings.set(scope, {server, scope});
    this.emit('server', server, scope);
    return this;
  }

  requestHandler() {
    return (req, res, next) => {
      const binding = req.method === 'POST' ?
        this._bindings.get(req.url.substr(1)) :
        undefined;
      if (!binding) {
        if (next) {
          next(); // Express.
        } else {
          res.writeHead(404);
          res.end();
        }
        return;
      }
      res.setHeader('Content-Type', 'text/plain');
      this._receiver(req.headers, (err) => {
        if (err) {
          res.writeHead(403);
          res.end(err.message);
          return;
        }
        const server = binding.server;
        const client = server.service.createClient({strictTypes: true});
        const channel = createInMemoryChannel(client, server);
        this.emit('channel', channel); // Transient channel.
        parseBody(req, server.service, function (err, jreq) {
          if (err) {
            res.writeHead(400);
            res.end(err.message);
            return;
          }
          res.setHeader('Content-Type', 'application/json');
          const jres = {};
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
                jres.headers = JSON.parse(
                  HEADERS_TYPE.toString(wres.headers)
                );
                prev();
              });
            })
            .emitMessage(jreq.message, jreq.request, function (err, res_) {
              channel.destroy();
              const msg = this.message;
              if (err !== undefined) {
                jres.error = JSON.parse(msg.errorType.toString(err));
              } else {
                jres.response = JSON.parse(msg.responseType.toString(res_));
              }
              res.end(JSON.stringify(jres));
            });
        });
      });
    };
  }

  tunnelHandler() {
    return (req, sock, head) => {
      const bindings = this._scopedBindings(req.headers.scopes);
      if (!bindings) {
        // At least one missing.
        socketError(sock, '404 Not Found');
        return;
      }
      if (head.length) {
        socketError(sock, '400 Bad Request', 'unsupported trailing data');
        return;
      }
      this._receiver(req.headers, (err) => {
        if (err) {
          socketError(sock, '403 Forbidden', err.message);
          return;
        }
        sock.write('HTTP/1.1 200 Connection Established\r\n\r\n');
        for (const binding of bindings) {
          debug('binding to scope %s', binding.scope);
          const channel = binding.server.createChannel(
            sock,
            {scope: binding.scope}
          );
          this.emit('channel', channel, sock); // Permanent channel.
        }
      });
    };
  }

  webSocketHandler(opts) {
    const wsServer = new ws.Server({noServer: true});
    return (req, sock, head) => {
      const bindings = this._scopedBindings(req.headers.scopes);
      if (!bindings) {
        // At least one missing.
        socketError(sock, '404 Not Found');
        return;
      }
      this._receiver(req.headers, (err) => {
        if (err) {
          socketError(sock, '403 Forbidden', err.message);
          return;
        }
        wsServer.handleUpgrade(req, sock, head, (client) => {
          for (const binding of bindings) {
            debug('binding to scope %s', binding.scope);
            const channel = binding.server.createChannel(
              wsStream(client, opts),
              {scope: binding.scope}
            );
            this.emit('channel', channel, sock); // Permanent channel.
          }
        });
      });
    };
  }

  _scopedBindings(scopes) {
    if (!scopes) {
      scopes = [''];
    }
    const bindings = [];
    for (const scope of scopes) {
      const binding = this._bindings.get(scope);
      if (!binding) {
        return undefined;
      }
      bindings.push(binding);
    }
    return bindings;
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
      sock.end();
      cb(new Error(head.toString() || res.statusMessage));
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
      try {
        obj.headers = HEADERS_TYPE.fromString(JSON.stringify(obj.headers));
      } catch (err) {
        // Do nothing, continue further to allow for a better error message.
      }
      try {
        obj.request = msg.requestType.fromString(JSON.stringify(obj.request));
      } catch (err) {
        // Idem.
      }
      cb(null, obj);
    });
}

/** Returns the _server's_ channel. */
function createInMemoryChannel(client, server) {
  const opts = {objectMode: true};
  const transports = [
    new stream.PassThrough(opts),
    new stream.PassThrough(opts)
  ];
  client.createChannel(
    {readable: transports[0], writable: transports[1]},
    opts
  );
  return server.createChannel(
    {readable: transports[1], writable: transports[0]},
    opts
  );
}

module.exports = {
  createProxy,
  startTunnel
};
