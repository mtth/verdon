/* jshint esversion: 6, node: true */

'use strict';

/** Functionality to enable tracing to clients and servers. */

const logicalTypes = require('./logical-types');

const avro = require('avsc');
const uuid = require('uuid');


// Default key used in locals and headers to store the trace.
const TRACE_KEY = 'trace';

// Trace type, serialized in message headers.
const TRACE_TYPE = avro.Type.forSchema(avro.readSchema(`
  record verdon.Trace {
    fixed Uuid(16) uuid;
    array record Call {
      string name;
      enum CallState { PENDING, FAILED, ERROR, SUCCESS } state;
      @logicalType("timestamp-ms") long requestTime;
      union { null, @logicalType("timestamp-ms") long } responseTime;
      array Call downstreamCalls = [];
    } calls = [];
  }
`), {logicalTypes});

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
        trace = createTrace();
      } else {
        next(new Error('missing trace'));
        return;
      }
    }
    const call = {
      name: this.getMessage().getName(),
      requestTime: new Date(),
      responseTime: null,
      state: 'PENDING',
      downstreamCalls: []
    };
    trace.calls.push(call);
    // We can save message size by not sending the downstream calls with the
    // request. This is without loss since they affect disjoint parts of the
    // trace.
    wreq.getHeader()[key] = TRACE_TYPE.toBuffer({uuid: trace.uuid});
    next(null, function (err, prev) {
      call.responseTime = new Date();
      if (err) {
        call.state = 'FAILED';
      } else if (wres.hasError()) {
        call.state = 'ERROR';
      } else {
        call.state = 'SUCCESS';
      }
      // Note that we don't propagate meta changes back, only forward.
      const traceBuf = wres.getHeader()[key];
      const downstreamCalls = TRACE_TYPE.fromBuffer(traceBuf).calls;
      Array.prototype.push.apply(call.downstreamCalls, downstreamCalls);
      this.getStub().getClient().emit('trace', trace);
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
        next(new Error('duplicate trace'));
        return;
      }
      locals[key] = TRACE_TYPE.fromBuffer(traceBuf);
    } else if (!locals[key]) {
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
