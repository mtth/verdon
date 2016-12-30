/* jshint esversion: 6, node: true */

'use strict';

const avro = require('avsc');


/** Custom logical type to represent dates as `Date` objects. */
class DateType extends avro.types.LogicalType {

  _fromValue(val) { return new Date(val); }

  _toValue(any) { return +any; }

  _resolve(type) {
    if (avro.Type.isType(type, 'long', 'string')) {
      return this._fromValue;
    }
  }
}


module.exports = {
  'timestamp-ms': DateType
};
