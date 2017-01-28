/* jshint esversion: 6, node: true */

'use strict';

const tracing = require('./tracing');
const utils = require('./utils');


module.exports = {
  promisify: utils.promisify,
  promisifyAll: utils.promisifyAll,
  tracing
};
