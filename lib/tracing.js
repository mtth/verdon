/* jshint esversion: 6, node: true */

'use strict';

/** Functionality to enable tracing to clients and servers. */

const utils = require('./utils');

const avro = require('avsc');
const uuid = require('uuid');
const util = require('util');

const debug = util.debuglog('verdon:tracing');

// Default key used in locals and headers to store the trace.
const TRACE_KEY = 'trace';

// Trace type, serialized in message headers.
const TRACE_TYPE = avro.Type.forSchema(avro.readSchema(`
  record verdon.Trace {
    fixed Uuid(16) uuid;
    array record Call {
      enum CallState { PENDING, ERROR, SUCCESS, ONE_WAY } state;
      string name;
      date requestTime;
      union { null, date } responseTime = null;
      array Call downstreamCalls = [];
    } calls = [];
  }
`, {typeRefs: utils.TYPE_REFS}), {logicalTypes: utils.LOGICAL_TYPES});

// Convenience import.
const Trace = TRACE_TYPE.recordConstructor;

/** Construct a new, empty, trace. */
function createTrace() {
  const buf = Buffer.alloc(16);
  return new Trace(uuid.v4(null, buf));
}

/**
 * Client tracing middleware.
 *
 * @param client {Client} Client to instrument.
 * @param opts {Object} Options;
 *  + traceKey {String} The key used to store the trace, both in the call's
 *    locals and the message headers.
 *  + createMissingOutgoing {Boolean} Whether to create the trace automatically
 *    if not provided when sending a message. By default this parameter is
 *    false, and emitting a message without a trace will fail (it can be easy
 *    to forget to do so otherwise).
 *  + ignoreMissingIncoming {Boolean} Whether to ignore responses which don't
 *    include a trace. This will lead to incomplete downstream call arrays.
 *    Defaults to false.
 */
function clientTracing(
    client,
    {
      traceKey = TRACE_KEY,
      createMissingOutgoing = false,
      ignoreMissingIncoming = false
    } = {}
  ) {
  client.on('channel', function (channel) {
    channel.on('outgoingCall', function (ctx, opts) {
      // Propagate the trace from the call's options to the context.
      ctx.locals[traceKey] = opts[traceKey];
    });
  });

  return function (wreq, wres, next) {
    let trace = this.locals[traceKey];
    if (!trace) {
      if (createMissingOutgoing) {
        debug('creating missing outgoing trace');
        trace = createTrace();
      } else {
        next(new Error('missing outgoing trace'));
        return;
      }
    }
    const call = {
      name: this.message.name,
      state: this.message.oneWay ? 'ONE_WAY' : 'PENDING',
      requestTime: new Date()
    };
    trace.calls.push(call);
    // We can save bandwidth by only sending the trace's UUID with the
    // request. This is without loss since they affect disjoint parts of the
    // trace.
    wreq.headers[traceKey] = TRACE_TYPE.toBuffer({uuid: trace.uuid});

    if (this.message.oneWay) {
      next();
    } else {
      next(null, function (err, prev) {
        call.responseTime = new Date();
        const traceBuf = wres.headers[traceKey];
        if (!traceBuf) {
          if (ignoreMissingIncoming) {
            debug('missing incoming trace');
          } else {
            throw new Error('missing incoming trace');
          }
        }
        let downstreamCalls;
        try {
          downstreamCalls = TRACE_TYPE.fromBuffer(traceBuf).calls;
        } catch (cause) {
          prev(err || cause);
          return;
        }
        call.state = (err || wres.error !== undefined) ? 'ERROR' : 'SUCCESS';
        call.downstreamCalls = downstreamCalls;
        prev(err);
      });
    }
  };
}

/**
 * Server tracing middleware.
 *
 * @param opts {Object} Options;
 *  + traceKey {String} The key used to store the trace, both in the call's
 *    locals and the message headers.
 */
function serverTracing(server, {traceKey = TRACE_KEY} = {}) {
  return function (wreq, wres, next) {
    if (wres === undefined) {
      // One-way message.
      next();
      return;
    }
    const traceBuf = wreq.headers[traceKey];
    const locals = this.locals;
    if (traceBuf) {
      if (locals[traceKey]) {
        next(new Error('duplicate trace'));
        return;
      }
      debug('reading incoming trace from headers');
      locals[traceKey] = TRACE_TYPE.fromBuffer(traceBuf);
    } else if (!locals[traceKey]) {
      debug('creating missing incoming trace');
      locals[traceKey] = createTrace();
    }
    next(null, function (err, prev) {
      wres.headers[traceKey] = TRACE_TYPE.toBuffer(locals[traceKey]);
      prev(err);
    });
  };
}

/** Combined middleware "installer". */
function enableTracing(opts) {
  return function (clientOrServer) {
    if (clientOrServer.emitMessage) {
      return clientTracing(clientOrServer, opts);
    } else if (clientOrServer.onMessage) {
      return serverTracing(clientOrServer, opts);
    } else {
      throw new TypeError(`unable to trace ${clientOrServer}`);
    }
  };
}


module.exports = {
  createTrace,
  enableTracing
};
