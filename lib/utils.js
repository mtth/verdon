/* jshint esversion: 6, node: true */

// TODO: Improve promisify methods (e.g. adding code-generation, or improving
// server processing: the `onMessage` method shouldn't be overridden).

'use strict';

/** Various utilities. */

const Promise = require('bluebird');
const avro = require('avsc');

// Type references used when assembling schemas.
const TYPE_REFS = {
  date: {type: 'long', logicalType: 'timestamp-ms'}
};

/** Custom logical type to represent dates as `Date` objects. */
class DateType extends avro.types.LogicalType {

  _fromValue(val) { return new Date(val); }

  _toValue(any) {
    if (any !== null && (any instanceof Date || !isNaN(any))) {
      return +any;
    }
  }

  _resolve(type) {
    if (avro.Type.isType(type, 'long', 'string')) {
      return this._fromValue;
    }
  }
}

/**
 * Transform (in-place) all named client RPC methods to also support promises.
 *
 * If no callback is passed when emitting a message, the handler will return a
 * promise. This promise will be bound to the call's context (similar to the
 * callback-based API). Additionally, the new methods will unwrap any remote
 * errors to enable the standard `.catch(MyError, fn)` bluebird idiom; see the
 * attached example for details.
 */
function promisifyClient(client) {
  // Note that we can't just promisify the method the standard way since we
  // would have no way to access the call's context (only present in the
  // request's callback). Hence the promise twiddling below.
  const originalFn = client.emitMessage;
  client.emitMessage = function (name, req, opts, cb) {
    if (cb || (!cb && typeof opts == 'function')) {
      return originalFn.call(this, name, req, opts, cb);
    }
    let reject, resolve, resolveCtx;
    const promise = new Promise(function (resolve_, reject_) {
      resolve = resolve_;
      reject = reject_;
    });
    const ctxPromise = new Promise(function (resolveCtx_) {
      resolveCtx = resolveCtx_; // Never rejected.
    });
    originalFn.call(this, name, req, opts, function (err, res) {
      resolveCtx(this);
      if (err !== undefined && err !== null) {
        // This will yield a false positive if someone defined `null` as valid
        // error type in the service's protocol. (Why would anyone do that?)
        if (typeof err.unwrapped == 'function') {
          const cause = err.unwrapped();
          if (cause.constructor.type.typeName === 'error') {
            err = cause;
          }
        }
        reject(err);
      } else {
        resolve(res);
      }
    });
    return promise.bind(ctxPromise);
  };
  return client;
}

/**
 * Transform (in-place) a server to also accept promise-based handlers.
 *
 * As a convenience, we also allow handlers to throw remote errors without
 * having to wrap them first (even when the message's errors type is a wrapped
 * union); see the attached example for details.
 */
function promisifyServer(server) {
  const fn = server.onMessage;
  server.onMessage = function (name, handler) {
    const msg = this.service.message(name);
    if (!msg || msg.oneWay || handler.length > 1) {
      return fn.call(this, name, handler);
    }
    const wrappedHandler = Promise.method(handler);
    return fn.call(this, name, function (req, cb) {
      wrappedHandler.call(this, req)
        .then(function (res) { cb(undefined, res); })
        .catch(function (err) {
          if (
            err && typeof err.wrapped == 'function' &&
            msg.errorType.typeName === 'union:wrapped'
          ) {
            // Allow handlers to throw custom error instances directly even
            // when the message's error type is a wrapped union.
            cb(err.wrapped());
          } else {
            cb(err);
          }
        });
    });
  };
  return server;
}

/**
 * Allow a server or client to support promise-based middleware handlers.
 *
 * If the handler returns a promise, the promise-based version will be used.
 */
function promisifyMiddleware(clientOrServer) {
  const fn = clientOrServer.use;
  clientOrServer.use = function (...handlers) {
    for (const handler_ of handlers) {
      // We might be dealing with dynamic middleware.
      const handler = handler_.length < 3 ? handler_(this) : handler_;
      fn.call(this, promisifyHandler(handler));
    }
    return this;
  };
  return clientOrServer;

  function promisifyHandler(handler) {
    const wrappedHandler = Promise.method(handler);
    return function (wreq, wres, next) {
      // Avoid creating the promise when possible.
      // TODO: Remove this optimization since we also use the argument to
      // determine dynamic/static middleware. So we should be doubly consistent
      // and not work when it isn't called.
      let promise, resolve, reject, prev;
      wrappedHandler.call(this, wreq, wres, function () {
        if (!promise) {
          promise = new Promise(function (resolve_, reject_) {
            resolve = resolve_;
            reject = reject_;
          });
          next(null, function (err, prev_) {
            prev = prev_;
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
        }
        return promise;
      }).then(function () { done(); }, done);

      function done(err) {
        if (!promise) {
          // TODO: Can we handle this better? It would be better to throw an
          // error here (happens if the function returns before next is
          // called).
          next(err);
        } else if (prev) {
          prev(err);
        } else {
          // Handler returned, but not ready yet.
          promise.then(done, done);
        }
      }
    };
  }
}

/** Public API to promisify a single client or server. */
function promisify(clientOrServer) {
  if (clientOrServer instanceof avro.Service.Client) {
    promisifyClient(clientOrServer);
  } else if (clientOrServer instanceof avro.Service.Server) {
    promisifyServer(clientOrServer);
  } else {
    throw new TypeError(`unable to promisify ${clientOrServer}`);
  }
  return promisifyMiddleware(clientOrServer);
}

/** Permanently and globally promisify clients and servers. */
function promisifyAll() {
  promisifyClient(avro.Service.Client.prototype);
  promisifyServer(avro.Service.Server.prototype);
  promisifyMiddleware(avro.Service.Client.prototype);
  promisifyMiddleware(avro.Service.Server.prototype);
}

module.exports = {
  LOGICAL_TYPES: {
    'timestamp-ms': DateType
  },
  TYPE_REFS,
  promisify,
  promisifyAll
};
