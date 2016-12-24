/* jshint esversion: 6, node: true */

'use strict';

const avro = require('avsc');
const net = require('net');
const stream = require('stream');
const request = require('request');
const util = require('util');
const {parse: parseUrl} = require('url');

const debug = util.debuglog('verdon');

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
      case 'file:':
        return require(obj.path);
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
    const svc = avro.Service.forProtocol(ptcl, {wrapUnions: true});
    const client = svc.createClient();
    client.createEmitter(transport, {noPing: isStateless, scope: opts.scope});
    cb(null, client);
  }
}

function runIfMain(cmd, module) {
  if (require.main === module) {
    cmd.parse(process.argv);
    if (!cmd.args.length) {
      cmd.emit('*', []);
    }
  }
  return cmd;
}

module.exports = {
  createClient,
  runIfMain
};
