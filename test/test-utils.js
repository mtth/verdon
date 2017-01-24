/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const utils = require('../lib/utils');

const assert = require('assert');
const avro = require('avsc');


suite('utils', function () {

  suite('promisify', function () {

    const svc = avro.Service.forProtocol({
      protocol: 'Math',
      messages: {
        neg: {request: [{name: 'n', type: 'int'}], response: 'int'},
        max: {
          request: [
            {name: 'n1', type: 'int'},
            {name: 'n2', type: 'int'}
          ],
          response: 'int'
        }
      }
    });

    let client, server;

    setup(function () {
      server = svc.createServer({silent: true});
      client = svc.createClient({server});
    });

    teardown(function () {
      client = undefined;
      server = undefined;
    });

    test('promisify client ok path underlying method', function (done) {
      utils.promisify(client);
      server.onNeg(function (n, cb) { cb(null, -n); });
      client.emitMessage('neg', {n: 2}, {}).then(function (n) {
        assert.equal(n, -2);
        done();
      });
    });

    test('promisify client ok path convenience handler', function (done) {
      utils.promisify(client);
      server.onNeg(function (n, cb) { cb(null, -n); });
      client.neg(2).then(function (n) {
        assert.equal(n, -2);
        done();
      });
    });

    test('promisify server throws system error', function (done) {
      utils.promisify(server);
      server.onNeg(function () { throw new Error('bar'); });
      client.neg(2, function (err) {
        assert(/bar/.test(err), err);
        done();
      });
    });

    test('promisify server ok response', function (done) {
      utils.promisify(server);
      server.onNeg(function (n) { return -n; });
      client.neg(2, function (err, n) {
        assert.ifError(err);
        assert.equal(n, -2);
        done();
      });
    });

    test('promisify server ok response skip argument', function (done) {
      utils.promisify(server);
      server.onMax(function (n1) { return n1; }); // Ignore second argument.
      client.max(2, 3, function (err, n) {
        assert.ifError(err);
        assert.equal(n, 2);
        done();
      });
    });

    test('promisify server middleware', function (done) {
      utils.promisify(server);
      const arr = [];
      server
        .use(function (wreq, wres, next) {
          arr.push('in');
          return next().then(function () {
            arr.push('out');
          });
        })
        .onNeg(function () {
          arr.push('on');
          throw new Error('bar');
        });
      client.neg(2, function (err) {
        assert(/bar/.test(err), err);
        assert.deepEqual(arr, ['in', 'on', 'out']);
        done();
      });
    });

    test('promisify server middleware error', function (done) {
      utils.promisify(server);
      const arr = [];
      server
        .use(function (wreq, wres, next) {
          arr.push('in');
          return next().catch(function () {
            arr.push('out');
            return null;
            // TODO: Check why the baz error is still propagated here. The null
            // should override it (and lead to a different error).
          });
        })
        .use(function (wreq, wres, next) {
          throw new Error('baz');
        });
      client.neg(2, function (err) {
        // assert(/baz/.test(err), err);
        assert.deepEqual(arr, ['in', 'out']);
        done();
      });
    });
  });
});
