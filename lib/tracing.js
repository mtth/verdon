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
  uuid.v4(buf);
  return new Trace(buf);
}

/**
 * Client tracing middleware.
 *
 * To use this middleware, the client should have been created with the
 * following call context option: `context: CallContext.forEmitter`.
 */
function clientTracing(client, opts) {
  opts = opts || {};
  const key = opts.traceKey || TRACE_KEY;
  const createMissing = !!opts.createMissing;

  client.on('stub', function (stub) {
    stub.on('outgoingCall', function (ctx, opts) {
      ctx.getLocals()[key] = opts[key];
    });
  });

  return function (wreq, wres, next) {
    let trace = this.getLocals()[key];
    if (!trace) {
      // If we automatically create missing traces, there is a high risk
      // that clients will forget to populate the trace.
      if (createMissing) {
        debug('creating missing outgoing trace');
        trace = createTrace();
      } else {
        next(new Error('missing outgoing trace'));
        return;
      }
    }
    const call = {name: this.getMessage().getName(), requestTime: new Date()};
    trace.calls.push(call);
    // We can save bandwidth by only sending the trace's UUID with the request.
    // This is without loss since they affect disjoint parts of the trace.
    wreq.getHeader()[key] = TRACE_TYPE.toBuffer({uuid: trace.uuid});
    next(null, function (err, prev) {
      call.responseTime = new Date();
      const traceBuf = wres.getHeader()[key];
      let downstreamCalls;
      try {
        downstreamCalls = TRACE_TYPE.fromBuffer(traceBuf).calls;
      } catch (err) {
        debug('missing incoming trace');
      }
      if (downstreamCalls) {
        if (err || wres.hasError()) {
          call.state = 'ERROR';
        } else {
          call.state = 'SUCCESS';
        }
        call.downstreamCalls = downstreamCalls;
      }
      prev(err);
    });
  };
}

/**
 * Server tracing middleware.
 *
 * To use this middleware, the server should have been created with the
 * following call context option: `context: CallContext.forListener`.
 */
function serverTracing(server, opts) {
  opts = opts || {};
  const key = opts.traceKey || TRACE_KEY;

  return function (wreq, wres, next) {
    const traceBuf = wreq.getHeader()[key];
    const locals = this.getLocals();
    if (traceBuf) {
      if (locals[key]) {
        wres.setError(new Error('duplicate trace'));
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
      wres.getHeader()[key] = TRACE_TYPE.toBuffer(locals[key]);
      prev(err);
    });
  };
}


module.exports = {
  clientTracing,
  createTrace,
  serverTracing
};
