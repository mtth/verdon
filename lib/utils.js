/* jshint esversion: 6, node: true */

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
 * Transform (in-place) client RPC methods to also support promises.
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
          if (
            cause && cause.constructor && cause.constructor.type &&
            cause.constructor.type.typeName === 'error'
          ) {
            // All this extraction logic to figure out whether we are dealing
            // with a custom error, in which case we unwrap it.
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
 * union).
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
        .catch(function (err) { cb(formatError(msg, err)); });
    });
  };
  return server;
}

/**
 * Takes any object and wraps it if it should be wrapped
 *
 * This allow handlers to throw custom error instances directly even
 * when the message's error type is a wrapped union.
 *
 * @param {Object} msg - schema for msg the error generated for
 * @param {*} err - object to check
 */
function formatError(msg, err) {
  const shouldWrap = err && typeof err.wrapped == 'function'
                  && msg.errorType.typeName === 'union:wrapped';

  if (shouldWrap) {
    return err.wrapped();
  }

  return err;
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
    const msg = this.message;
    return function (wreq, wres, next) {
      let reject, resolve, prev;
      const promise = new Promise(function (resolve_, reject_) {
        resolve = resolve_;
        reject = reject_;
      });
      let ret;
      try {
        ret = wrappedHandler.call(this, wreq, wres, (err, cb) => {
          if (err) {
            err = formatError(msg, err)
          }
          if (cb) {
            // Always use the callback API if one is provided here.
            next(err, cb);
            return;
          }
          next(err, function (err, prev_) {
            prev = prev_;
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
          return promise.bind(this);
        });
      } catch (err) {
        // If an error is thrown synchronously in the handler, we'll be
        // accommodating and assume that this is a promise's rejection.
        next(formatError(msg, err));
        return;
      }

      if (wres.error) {
        wres.error = formatError(msg, wres.error);
      }

      if (ret && typeof ret.then === 'function') {
        // Cheap way of testing whether `ret` is a promise. If so, we use the
        // promise-based API: we wait until the returned promise is complete to
        // trigger any backtracking.
        ret.then(done, done);
      } else {
        promise.then(done, done);
      }

      function done(err) {
        if (prev) {
          prev(formatError(msg, err));
        } else {
          // This will happen if the returned promise is complete before the
          // one returned by `next()` is. There is no clear ideal behavior
          // here, to be safe we will throw an error.
          const cause = new Error('early middleware return');
          promise.finally(function () { prev(cause); });
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
