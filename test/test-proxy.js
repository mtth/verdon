/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const proxy = require('../lib/proxy');

const assert = require('assert');
const avro = require('avsc');
const http = require('http');


suite('proxy', function () {

  const svc = avro.Service.forProtocol({
    protocol: 'Math',
    messages: {
      neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
    }
  });

  let client, server;

  setup(function () {
    client = svc.createClient();
    server = svc.createServer();
  });

  teardown(function () {
    client = undefined;
    server = undefined;
  });

  test('connect method', function (done) {
    const p = proxy.createProxy()
      .mount('/', server.onNeg(function (n, cb) { cb(null, -n); }));
    const httpServer = http.createServer();
    httpServer
      .on('connect', p.tunnelHandler())
      .on('listening', function () {
        proxy.startTunnel('/', {port: 8080}, function (err, tunnel) {
          assert.ifError(err);
          client.createChannel(tunnel);
          client.neg(2, function (err, n) {
            assert.ifError(err);
            assert.equal(n, -2);
            client.destroyChannels();
            httpServer.close();
          });
        });
      })
      .on('close', function () { done(); })
      .listen(8080);
  });

  test('connect method custom receiver ok', function (done) {
    let sawHeaders = false;
    let sawSocket = false;
    const p = proxy.createProxy(function (hdrs, cb) {
      assert.equal(hdrs.one, 1);
      sawHeaders = true;
      cb(null, function (channel, sock) {
        assert.strictEqual(channel.server.service, svc);
        sawSocket = !!sock;
      });
    }).mount('/', server.onNeg(function (n, cb) { cb(null, -n); }));
    const httpServer = http.createServer();
    httpServer
      .on('connect', p.tunnelHandler())
      .on('listening', function () {
        const opts = {headers: {one: 1}, port: 8080};
        proxy.startTunnel('/', opts, function (err, tunnel) {
          assert.ifError(err);
          client.createChannel(tunnel);
          client.neg(2, function (err, n) {
            assert.ifError(err);
            assert.equal(n, -2);
            client.destroyChannels();
            httpServer.close();
          });
        });
      })
      .on('close', function () { done(); })
      .listen(8080);
  });

  test('connect method custom receiver no', function (done) {
    const p = proxy.createProxy(function (hdrs, cb) {
      cb(new Error('foo'));
    }).mount('/', server);
    const httpServer = http.createServer();
    httpServer
      .on('connect', p.tunnelHandler())
      .on('listening', function () {
        proxy.startTunnel('/', {port: 8080}, function (err) {
          assert(/foo/.test(err), err);
          httpServer.close();
        });
      })
      .on('close', function () { done(); })
      .listen(8080);
  });

  test('connect method missing mount', function (done) {
    const p = proxy.createProxy()
      .mount('/', server.onNeg(function (n, cb) { cb(null, -n); }));
    const httpServer = http.createServer();
    httpServer
      .on('connect', p.tunnelHandler())
      .on('listening', function () {
        proxy.startTunnel('/foo', {port: 8080}, function (err) {
          assert(/Not Found/.test(err), err);
          httpServer.close();
        });
      })
      .on('close', function () { done(); })
      .listen(8080);
  });

  test('no connect method', function (done) {
    const httpServer = http.createServer();
    httpServer
      .on('listening', function () {
        proxy.startTunnel('/', {port: 8080}, function (err) {
          assert(/socket hang up/.test(err), err);
          httpServer.close();
        });
      })
      .on('close', function () { done(); })
      .listen(8080);
  });

  test('post method ok', function (done) {
    const p = proxy.createProxy()
      .mount('/', server.onNeg(function (n, cb) { cb(null, -n); }));
    const httpServer = http.createServer();
    httpServer
      .on('request', p.requestListener())
      .on('listening', function () {
        http.request({method: 'POST', port: 8080})
          .on('response', function (res) {
            assert.equal(res.statusCode, 200);
            httpServer.close();
          })
          .end('{"message":"neg","request":{"n":2}}');
      })
      .on('close', function () { done(); })
      .listen(8080);
  });

  test('post method missing message', function (done) {
    const p = proxy.createProxy()
      .mount('/', server.onNeg(function (n, cb) { cb(null, -n); }));
    const httpServer = http.createServer();
    httpServer
      .on('request', p.requestListener())
      .on('listening', function () {
        http.request({method: 'POST', port: 8080})
          .on('response', function (res) {
            assert.equal(res.statusCode, 400);
            httpServer.close();
          })
          .end('{"message":"plus","request":{}}');
      })
      .on('close', function () { done(); })
      .listen(8080);
  });
});
