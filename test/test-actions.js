/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const actions = require('../lib/actions');
const assert = require('assert');
const avro = require('avsc');
const stream = require('stream');


suite('actions', function () {

  suite('assemble', function () {

    test('existing protocol', function (done) {
      actions.assemble('1.avdl', undefined, {importHook}, function (err, str) {
        assert.ifError(err);
        assert.equal(str, '{"protocol":"Empty"}');
        done();
      });

      function importHook(path, kind, cb) {
        assert.equal(path, '1.avdl');
        assert.equal(kind, 'idl');
        cb(null, 'protocol Empty{}');
      }
    });

    test('existing type', function (done) {
      actions.assemble('1.avdl', 'Bar', {importHook}, function (err, str) {
        assert.ifError(err);
        assert.equal(str, '{"name":"Bar","type":"fixed","size":1}');
        done();
      });

      function importHook(path, kind, cb) {
        assert.equal(path, '1.avdl');
        assert.equal(kind, 'idl');
        cb(null, 'protocol Foo{fixed Bar(1);}');
      }
    });

    test('missing type', function (done) {
      actions.assemble('1.avdl', 'Bar', {importHook}, function (err) {
        assert(/no such type/.test(err), err);
        done();
      });

      function importHook(path, kind, cb) {
        assert.equal(path, '1.avdl');
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
        .createChannel(tps[1]);
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
      svc.createServer().createChannel(tps[1]);
      actions.call(tps[0], 'lower', '{}', {}, function (err) {
        assert(/no such message/.test(err), err);
        done();
      });
    });

    test('send header', function (done) {
      const tps = createTransports();
      const ptcl = avro.readProtocol('protocol Echo{string upper(string s);}');
      const svc = avro.Service.forProtocol(ptcl);
      const stringType = avro.Type.forSchema('string');
      const val = 'bar';
      const buf = stringType.toBuffer(val);
      let sawHeader = false;
      svc.createServer()
        .use(function (wreq, wres, next) {
          assert(buf.equals(wreq.headers.foo));
          sawHeader = true;
          next();
        })
        .onUpper(function (str, cb) { cb(null, str.toUpperCase()); })
        .createChannel(tps[1]);
      const opts = {
        header: [{key: 'foo', name: 'string', val: stringType.toString(val)}]
      };
      actions.call(tps[0], 'upper', '{"s": "abc"}', opts, function (err, str) {
        assert.ifError(err);
        assert.equal(str, '"ABC"');
        assert(sawHeader);
        done();
      });
    });
  });

  suite('info', function () {

    test('message JSON', function (done) {
      const tps = createTransports();
      const ptcl = avro.readProtocol('protocol Ping { int ping(); }');
      const svc = avro.Service.forProtocol(ptcl);
      svc.createServer().createChannel(tps[1]);
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
      svc.createServer().createChannel(tps[1]);
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
      svc.createServer().createChannel(tps[1]);
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
      svc.createServer().createChannel(tps[1]);
      actions.createClient(tps[0], {}, function (err, client) {
        assert.equal(err, null);
        assert.deepEqual(client.service.protocol, ptcl);
      });
    });

    test('local protocol', function (done) {
      const tps = createTransports();
      const opts = {protocol: 'empty.avdl', importHook};
      const ptcl = {protocol: 'Empty'};
      actions.createClient(tps[0], opts, function (err, client) {
        assert.equal(err, null);
        assert.deepEqual(client.service.protocol, ptcl);
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
