/* jshint esversion: 6, node: true */

'use strict';

const avro = require('avsc');
const net = require('net');
const stream = require('stream');
const url = require('url');
const request = require('request');
const util = require('util');

const debug = util.debuglog('avro-rpc');

/**
 * Retrieve information about RPCs for a given service.
 *
 * @param `cfg` {Object} Service configuration, the following keys are
 * supported:
 *
 * + `address`, required.
 * + `scope`, optional.
 * + `path`, optional.
 */
function info(cfg, cb) {
  debug('gathering infos');
  getClient(cfg, (err, client) => {
    if (err) {
      cb(err);
      return;
    }
    client.getEmitters().forEach((emitter) => { emitter.destroy(); });
    cb(null, client.getService());
  });
}

/**
 * Emit an RPC.
 *
 * See above for `cfg` details.
 */
function sendMessage(cfg, name, jsonReq, cb) {
  getClient(cfg, (err, client) => {
    if (err) {
      cb(err);
      return;
    }
    const msg = client.getProtocol().getMessage(name);
    if (!msg) {
      cb(new Error(`no such message: ${name}`));
    }
    const req = msg.getRequestType().fromString(jsonReq);
    client.emitMessage(name, req, function (err, res) {
      this.destroy();
      if (err) {
        cb(err);
        return;
      }
      const jsonRes = msg.isOneWay() ?
        undefined :
        msg.getResponseType().toString(res);
      cb(null, jsonRes);
    });
  });
}

/**
 * Helper method to generate an emitter from the given service configuration.
 *
 * If no local protocol is given, we must perform a little gymnastic to retrive
 * the remote protocol without consuming the transport early.
 */
function getClient(cfg, cb) {
  const transport = (function () {
    const options = url.parse(cfg.address);
    switch (options.protocol || 'http:') {
      case 'http:':
      case 'https:':
        return (cb) => {
          const bufs = [];
          return new stream.Writable({
            write: (buf, encoding, cb) => {
              bufs.push(buf);
              cb();
            }
          }).on('finish', () => {
            const body = Buffer.concat(bufs);
            debug('sending request with %d bytes', body.length);
            request.post({
              url: cfg.address,
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
        return net.createConnection({
          port: options.port,
          host: options.hostname
        });
      default:
        return undefined;
    }
  })();
  if (!transport) {
    cb(new Error(`invalid address: ${cfg.address}`));
    return;
  }

  const isStateless = typeof transport == 'function';
  if (cfg.path) {
    debug('assembling protocol');
    avro.assembleProtocolSchema(cfg.path, done);
  } else {
    debug('discovering protocol');
    avro.discoverProtocol(transport, {scope: cfg.scope}, done);
  }

  function done(err, ptcl) {
    if (err) {
      cb(err);
      return;
    }
    const svc = avro.Service.forProtocol(ptcl, {wrapUnions: true});
    const client = svc.createClient();
    client.createEmitter(transport, {noPing: isStateless, scope: cfg.scope});
    cb(null, client);
  }
}

module.exports = {
  info,
  sendMessage
};
