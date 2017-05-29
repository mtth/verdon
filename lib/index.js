/* jshint esversion: 6, node: true */

'use strict';

const proxy = require('./proxy');
const tracing = require('./tracing');
const utils = require('./utils');


module.exports = {
  clientTracing: tracing.clientTracing,
  createProxy: proxy.createProxy,
  createTrace: tracing.createTrace,
  promisify: utils.promisify,
  promisifyAll: utils.promisifyAll,
  serverTracing: tracing.serverTracing,
  startTunnel: proxy.startTunnel
};
