/* jshint esversion: 8, node: true */

'use strict';

const {promisifyAll} = require('../lib');

const avro = require('avsc');
const Promise = require('bluebird');

// Enable promisified client and servers for all services.
promisifyAll(avro.Service);

const service = avro.Service.forProtocol({
  protocol: 'HelloService',
  messages: {
    hello: {
      request: [{name: 'name', type: 'string'}],
      response: 'string',
    },
  },
});

const server = service.createServer()
  .onHello(async function (name) {
    await Promise.delay(1000);
    return `hello ${name}`;
  });

const client = service.createClient({buffering: true, server});

async function main() {
  const result = await client.hello('Martin');
  console.log(result);
}

main();
