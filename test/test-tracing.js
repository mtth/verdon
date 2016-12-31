/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const tracing = require('../lib/tracing');

const assert = require('assert');
const avro = require('avsc');


suite('tracing', function () {

  const svc = avro.Service.forProtocol({
    protocol: 'Math',
    messages: {
      neg: {request: [{name: 'n', type: 'int'}], response: 'int'},
      abs: {request: [{name: 'n', type: 'int'}], response: 'int'}
    }
  });

  let client, server;

  setup(function () {
    server = createServer();
    client = createClient(server);
  });

  teardown(function () {
    client = undefined;
    server = undefined;
  });

  test('direct round-trip', function (done) {
    const trace = tracing.createTrace();
    server.onNeg(function (n, cb) {
      assert(this.locals.trace.uuid.equals(trace.uuid));
      assert.strictEqual(this.channel.server, server);
      cb(null, -n);
    });
    client.neg(10, {trace}, function (err, n) {
      assert.ifError(err);
      assert.equal(n, -10);
      assert.strictEqual(this.locals.trace, trace);
      assert.strictEqual(this.channel.client, client);
      assert.equal(trace.calls.length, 1);
      const call = trace.calls[0];
      assert.equal(call.name, 'neg');
      assert.equal(call.state, 'SUCCESS');
      assert.equal(call.downstreamCalls.length, 0);
      done();
    });
  });

  test('single hop round-trip', function (done) {
    server.onNeg(function (n, cb) {
        cb(null, -n);
      });
    const hopServer = createServer()
      .onNeg(function (n, cb) {
        // Delegate, but then fail.
        client.neg(n, {trace: this.locals.trace}, function (err, res) {
          assert.equal(res, -20);
          cb(new Error('bar'));
        });
      });
    createClient(hopServer)
      .once('channel', function () {
        const trace = tracing.createTrace();
        this.neg(20, {trace}, function (err) {
          assert(/bar/.test(err), err);
          assert.strictEqual(this.locals.trace, trace);
          assert.equal(trace.calls.length, 1);
          const call = trace.calls[0];
          assert.equal(call.state, 'ERROR');
          assert.equal(call.downstreamCalls.length, 1);
          const downstreamCall = call.downstreamCalls[0];
          assert.equal(downstreamCall.state, 'SUCCESS');
          done();
        });
      });
  });

  test('missing outgoing trace', function (done) {
    client.neg(5, function (err) {
      assert(/missing outgoing trace/.test(err), err);
      done();
    });
  });

  test('duplicate trace', function (done) {
    server.once('channel', function (channel) {
      channel.on('incomingCall', function (ctx) {
        ctx.locals.trace = tracing.createTrace(); // Pre-populate a trace.
      });
    });
    client.neg(3, {trace: tracing.createTrace()}, function (err) {
      assert(/duplicate trace/.test(err), err);
      done();
    });
  });

  function createClient(server) {
    return svc.createClient({server}).use(tracing.clientTracing());
  }

  function createServer() {
    return svc.createServer({silent: true}).use(tracing.serverTracing());
  }
});
