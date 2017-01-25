/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const utils = require('../lib/utils');

const Promise = require('bluebird');
const assert = require('assert');
const avro = require('avsc');


suite('utils', function () {

  suite('promisify', function () {

    const svc = avro.Service.forProtocol({
      protocol: 'Math',
      messages: {
        neg: {
          request: [{name: 'n', type: 'int'}],
          response: 'int',
          errors: [
            {
              type: 'error',
              name: 'MathError',
              fields: [{name: 'code', type: 'int'}]
            }
          ]
        },
        max: {
          request: [
            {name: 'n1', type: 'int'},
            {name: 'n2', type: 'int'}
          ],
          response: 'int'
        }
      }
    }, {wrapUnions: true});

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

    test('promisify client catch wrapped custom error', function (done) {
      utils.promisify(client);
      const MathError = svc.type('MathError').recordConstructor;
      server.onNeg(function (n, cb) { cb({MathError: {code: 123}}); });
      client.neg(2).catch(MathError, function (err) {
        assert.equal(err.code, 123);
        assert.strictEqual(this.channel.client, client);
        done();
      });
    });

    test('promisify client catch wrapped custom error', function (done) {
      utils.promisify(client);
      const err = new Error('bar');
      server.onNeg(function (n, cb) { cb(err); });
      client.neg(2).catch(function (err_) {
        assert(err_ instanceof Error);
        assert(/bar/.test(err_), err_);
        done();
      });
    });

    test('promisify client callback api', function (done) {
      utils.promisify(client);
      server.onNeg(function (n, cb) { cb(null, -n); });
      client.neg(2, function (err, n) {
        assert.ifError(err);
        assert.equal(n, -2);
        assert.strictEqual(this.channel.client, client);
        done();
      });
    });

    test('promisify client middleware early return', function (done) {
      utils.promisify(client);
      server.onNeg(function (n, cb) { cb(null, -n); });
      client
        .use(function (wreq, wres, next) {
          wreq.request = {n: 1};
          this.locals.one = 1;
          setTimeout(function () {
            next();
          }, 10);
          return Promise.reject(new Error('bar'));
        })
        .neg(2).catch(function (err) {
          assert(/early/.test(err), err);
          assert.strictEqual(this.locals.one, 1);
          done();
        });
    });

    test('promisify server throw system error', function (done) {
      utils.promisify(server);
      server.onNeg(function () { throw new Error('bar'); });
      client.neg(2, function (err) {
        assert(/bar/.test(err), err);
        done();
      });
    });

    test('promisify server throw custom error', function (done) {
      utils.promisify(server);
      const MathError = svc.type('MathError').recordConstructor;
      server.onNeg(function () { throw new MathError(123); });
      client.neg(2, function (err) {
        assert.equal(err.MathError.code, 123);
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

    test('promisify server callback api', function (done) {
      utils.promisify(server);
      server.onMax(function (n1, n2, cb) { cb(null, Math.max(n1, n2)); });
      client.max(2, 3, function (err, n) {
        assert.ifError(err);
        assert.equal(n, 3);
        done();
      });
    });

    test('promisify server middleware propagate error', function (done) {
      utils.promisify(server);
      const arr = [];
      server
        .use(function (wreq, wres, next) {
          arr.push('in');
          return next().catch(function (err) {
            arr.push('out');
            wres.error = {string: err.message};
          });
        })
        .use(function (wreq, wres, next) {
          arr.push('on');
          if (true) {
            throw new Error('bar');
          } else {
            // Avoid the `next` unused argument.
            next();
          }
        });
      client.neg(2, function (err) {
        assert(/bar/.test(err), err);
        assert.deepEqual(arr, ['in', 'on', 'out']);
        done();
      });
    });

    test('promisify server middleware swallow error', function (done) {
      utils.promisify(server);
      const arr = [];
      server
        .use(function (wreq, wres, next) {
          arr.push('in');
          return next().catch(function () {
            arr.push('out');
            wres.response = -3;
          });
        })
        .use(function (wreq, wres, next) {
          arr.push('on');
          next(new Error('baz'), function () {});
        });
      client.neg(2, function (err, n) {
        assert.ifError(err);
        assert.equal(n, -3);
        assert.deepEqual(arr, ['in', 'on', 'out']);
        done();
      });
    });

    test('promisify server middleware', function (done) {
      utils.promisify(server);
      server
        .use(function (wreq, wres, next) {
          assert.strictEqual(this.channel.server, server);
          wreq.request = {n: 1};
          return next().then(function () {
            assert.strictEqual(this.channel.server, server);
            wres.response = -1;
          });
        })
        .onNeg(function (n, cb) {
          assert.equal(n, 1);
          cb(null, -2);
        });
      client.neg(2, function (err, n) {
        assert.ifError(err);
        assert.equal(n, -1);
        done();
      });
    });

    test('promisify server middleware callback api', function (done) {
      utils.promisify(server);
      let called = false;
      server
        .use(function (wreq, wres, next) {
          next().then(function () {
            called = true;
          });
          // Don't return anything so the callback API will be used.
        })
        .onNeg(function (n) {
          return -n;
        });
      client.neg(2, function (err, n) {
        assert.ifError(err);
        assert.equal(n, -2);
        assert(called);
        done();
      });
    });
  });
});
