/* jshint esversion: 6, node: true */

'use strict';

/** Indent string. */
function indent(str, n) { return str.replace(/^/gm, ' '.repeat(n | 0)); }

module.exports = {
  indent
};
