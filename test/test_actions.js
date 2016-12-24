/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const actions = require('../lib/actions');
const assert = require('assert');
const avro = require('avsc');
const stream = require('stream');


suite('actions', function () {

  suite('assemble', function () {

    test('existing protocol', function (done) {
      actions.assemble('empty.avdl', {importHook}, function (err, str) {
        assert.ifError(err);
        assert.equal(str, '{"protocol":"Empty"}');
        done();
      });

      function importHook(path, kind, cb) {
        assert.equal(path, 'empty.avdl');
        assert.equal(kind, 'idl');
        cb(null, 'protocol Empty{}');
      }
    });

  });

  suite('call', function () {

    test('same protocol', function (done) {
      const tps = createTransports();
      const ptcl = avro.readProtocol('protocol Echo{string upper(string s);}');
      const svc = avro.Service.forProtocol(ptcl);
      svc.createServer()
        .onUpper(function (str, cb) { cb(null, str.toUpperCase()); })
        .createListener(tps[1]);
      actions.call(tps[0], 'upper', '{"s": "ab"}', {}, function (err, str) {
        assert.ifError(err);
        assert.equal(str, '"AB"');
        done();
      });
    });

    test('unknown message', function (done) {
      const tps = createTransports();
      const ptcl = avro.readProtocol('protocol Echo{string upper(string s);}');
      const svc = avro.Service.forProtocol(ptcl);
      svc.createServer().createListener(tps[1]);
      actions.call(tps[0], 'lower', '{}', {}, function (err) {
        assert(/no such message/.test(err), err);
        done();
      });
    });
  });

  suite('info', function () {

    test('message JSON', function (done) {
      const tps = createTransports();
      const ptcl = avro.readProtocol('protocol Ping { int ping(); }');
      const svc = avro.Service.forProtocol(ptcl);
      svc.createServer().createListener(tps[1]);
      actions.info(tps[0], 'ping', {json: true}, function (err, str) {
        assert.ifError(err);
        assert.equal(str, JSON.stringify(ptcl.messages.ping));
        done();
      });
    });

    test('protocol JSON', function (done) {
      const tps = createTransports();
      const ptcl = avro.readProtocol('protocol Ping { int ping(); }');
      const svc = avro.Service.forProtocol(ptcl);
      svc.createServer().createListener(tps[1]);
      actions.info(tps[0], undefined, {json: true}, function (err, str) {
        assert.ifError(err);
        assert.equal(str, JSON.stringify(ptcl));
        done();
      });
    });

    test('missing message', function (done) {
      const tps = createTransports();
      const ptcl = avro.readProtocol('protocol Ping { int ping(); }');
      const svc = avro.Service.forProtocol(ptcl);
      svc.createServer().createListener(tps[1]);
      actions.info(tps[0], 'pong', {json: true}, function (err) {
        assert(/no such message/.test(err), err);
        done();
      });
    });
  });

  suite('create client', function () {

    test('remote protocol', function () {
      const tps = createTransports();
      const ptcl = avro.readProtocol('protocol Ping { int ping(); }');
      const svc = avro.Service.forProtocol(ptcl);
      svc.createServer().createListener(tps[1]);
      actions.createClient(tps[0], {}, function (err, client) {
        assert.equal(err, null);
        assert.deepEqual(client.getService().getProtocol(), ptcl);
      });
    });

    test('local protocol', function (done) {
      const tps = createTransports();
      const opts = {protocol: 'empty.avdl', importHook};
      const ptcl = {protocol: 'Empty'};
      actions.createClient(tps[0], opts, function (err, client) {
        assert.equal(err, null);
        assert.deepEqual(client.getService().getProtocol(), ptcl);
        done();
      });

      function importHook(path, kind, cb) {
        assert.equal(path, 'empty.avdl');
        assert.equal(kind, 'idl');
        cb(null, 'protocol Empty{}');
      }
    });
  });
});

function createTransports() {
  const duplex1 = new stream.PassThrough();
  const duplex2 = new stream.PassThrough();
  return [
    {readable: duplex1, writable: duplex2},
    {readable: duplex2, writable: duplex1}
  ];
}
