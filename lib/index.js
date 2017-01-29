/* jshint esversion: 6, node: true */

'use strict';

const proxy = require('./proxy');
const tracing = require('./tracing');
const utils = require('./utils');


module.exports = {
  createProxy: proxy.createProxy,
  createTrace: tracing.createTrace,
  enableTracing: tracing.enableTracing,
  promisify: utils.promisify,
  promisifyAll: utils.promisifyAll,
  startTunnel: proxy.startTunnel
};
