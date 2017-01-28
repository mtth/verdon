/* jshint esversion: 6, node: true */

// TODO: Allow support for late server mounting. Or some way of indicating
// which servers are ready and which aren't. Related to the proxy's status: OK
// only when all servers have been mounted. This will be done as follows:
// Servers will be registered separately (likely on an "envelope" TBD) and this
// envelope will be started with a list of services that it should expose. The
// status will be OK when all these services' servers have been registered
// (registration also mounts them).

'use strict';

/** Infrastructure. */

const http = require('http');
const stream = require('stream');


/** HTTP service proxy. */
class HttpProxy {
  constructor(receiver, opts) {
    opts = opts || {};
    this.mounts = new Map();
    this.server =  (opts.server || http.createServer())
      .on('connect', (req, sock, head) => {
        const mount = this.mounts.get(req.url);
        if (!mount) {
          tunnelError(sock, '404 Not Found');
          return;
        }
        if (head.length) {
          tunnelError(sock, '400 Bad Request', 'unsupported trailing data');
          return;
        }
        receiver(req.headers, function (err, cb) {
          if (err) {
            tunnelError(sock, '403 Forbidden', err.message);
            return;
          }
          sock.write('HTTP/1.1 200 Connection Established\r\n\r\n');
          const channel = mount.server.createChannel(sock);
          mount.addConnection(sock, channel);
          if (cb) {
            cb(channel);
          }
        });
      })
      .on('request', (req, res) => {
        if (req.method !== 'POST') {
          return;
        }
        res.setHeader('Content-Type', 'text/plain');
        const mount = this.mounts.get(req.url);
        if (!mount) {
          res.writeHead(404);
          res.end();
          return;
        }
        receiver(req.headers, function (err, cb) {
          if (err) {
            res.writeHead(403);
            res.end(err.message);
            return;
          }
          const service = mount.server.service;
          const client = service.createClient({strictTypes: true});
          const channel = createInMemoryChannel(client, mount.server);
          if (cb) {
            cb(channel);
          }
          parseBody(req, service, function (err, jreq) {
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
                  jres.headers = wres.headers;
                  prev();
                });
              })
              .emitMessage(jreq.message, jreq.request, function (err, res_) {
                if (err) {
                  jres.error = err;
                } else {
                  jres.response = res_;
                }
                res.end(JSON.stringify(jres));
              });
          });
        });
      });
  }

  mount(path, server) {
    this.mounts.set(path, new Mount(server));
    return this;
  }
}

/** Convenience struct. */
class Mount {
  constructor(server) {
    this.server = server;
    this.activeConnections = [];
  }

  addConnection(sock, channel) {
    const conns = this.activeConnections;
    const conn = {socket: sock, channel};
    conns.push(conn);
    sock.on('finish', function () {
      conns.splice(conns.indexOf(conn), 1);
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
function startTunnel(path, opts, cb) {
  if (!cb && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }
  opts = opts || {};
  http.request({
    path,
    host: opts.hostname,
    port: opts.port,
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
function tunnelError(sock, status, msg) {
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
      let jreq;
      try {
        jreq = JSON.parse(str);
      } catch (err) {
        cb(err);
        return;
      }
      const msg = service.message(jreq.message);
      if (!msg) {
        cb(new Error(`unknown message: ${jreq.message}`));
        return;
      }
      try {
        // This is required (though obviously non-optimal) to properly support
        // buffers and consistently support unions.
        jreq.request = msg.requestType.fromString(JSON.stringify(jreq.request));
      } catch (err) {
        cb(err);
        return;
      }
      cb(null, jreq);
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
