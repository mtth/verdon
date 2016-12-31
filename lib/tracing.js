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
      enum CallState { PENDING, ERROR, SUCCESS } state = "PENDING";
      string name;
      @logicalType("timestamp-ms") long requestTime;
      union { null, @logicalType("timestamp-ms") long } responseTime = null;
      union { null, array Call } downstreamCalls = null;
    } calls = [];
  }
`), {logicalTypes: utils.logicalTypes});

// Convenience import.
const Trace = TRACE_TYPE.getRecordConstructor();

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
 *  + createMissing {Boolean} Whether to create the trace automatically if not
 *    provided when sending a message. By default this parameter is false, and
 *    emitting a message without a trace will fail (it can be easy to forget to
 *    do so otherwise).
 */
function clientTracing(opts) {
  opts = opts || {};
  const key = opts.traceKey || TRACE_KEY;
  const createMissing = !!opts.createMissing;

  return function (client) {
    client.on('channel', function (channel) {
      channel.on('outgoingCall', function (ctx, opts) {
        // Propagate the trace from the call's options to the context.
        ctx.locals[key] = opts[key];
      });
    });

    return function (wreq, wres, next) {
      let trace = this.locals[key];
      if (!trace) {
        if (createMissing) {
          debug('creating missing outgoing trace');
          trace = createTrace();
        } else {
          next(new Error('missing outgoing trace'));
          return;
        }
      }
      const call = {name: this.message.name, requestTime: new Date()};
      trace.calls.push(call);
      // We can save bandwidth by only sending the trace's UUID with the
      // request. This is without loss since they affect disjoint parts of the
      // trace.
      wreq.headers[key] = TRACE_TYPE.toBuffer({uuid: trace.uuid});
      next(null, function (err, prev) {
        call.responseTime = new Date();
        const traceBuf = wres.headers[key];
        let downstreamCalls;
        try {
          downstreamCalls = TRACE_TYPE.fromBuffer(traceBuf).calls;
        } catch (cause) {
          debug('missing incoming trace');
        }
        if (downstreamCalls) {
          call.state = (err || wres.error !== undefined) ? 'ERROR' : 'SUCCESS';
          call.downstreamCalls = downstreamCalls;
        }
        prev(err);
      });
    };
  };
}

/**
 * Server tracing middleware.
 *
 * @param opts {Object} Options;
 *  + traceKey {String} The key used to store the trace, both in the call's
 *    locals and the message headers.
 */
function serverTracing(opts) {
  opts = opts || {};
  const key = opts.traceKey || TRACE_KEY;

  return function (wreq, wres, next) {
    const traceBuf = wreq.headers[key];
    const locals = this.locals;
    if (traceBuf) {
      if (locals[key]) {
        wres.error = new Error('duplicate trace');
        next();
        return;
      }
      debug('reading incoming trace from headers');
      locals[key] = TRACE_TYPE.fromBuffer(traceBuf);
    } else if (!locals[key]) {
      debug('creating missing incoming trace');
      locals[key] = createTrace();
    }
    next(null, function (err, prev) {
      wres.headers[key] = TRACE_TYPE.toBuffer(locals[key]);
      prev(err);
    });
  };
}


module.exports = {
  clientTracing,
  createTrace,
  serverTracing
};
