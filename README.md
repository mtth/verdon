# Verdon [![NPM version](https://img.shields.io/npm/v/verdon.svg)](https://www.npmjs.com/package/verdon) [![Build status](https://travis-ci.org/mtth/verdon.svg?branch=master)](https://travis-ci.org/mtth/verdon) [![Coverage status](https://coveralls.io/repos/mtth/verdon/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/verdon?branch=master)

Avro services utilities.

## Proxies

Sample Avro remote logging service running behind a WebSocket server:

```javascript
const avro = require('avsc');
const http = require('http');
const verdon = require('verdon');

const protocol = avro.readProtocol(`
  protocol RemoteLogService {
    enum Level { DEBUG, INFO, WARNING }
    void log(Level level, string message);
  }
`);

const logServer = avro.Service.forProtocol(protocol).createServer()
  .onLog(function (level, msg) { console.log(`${level}: ${msg}`); });

const proxy = verdon.createProxy().bind(logServer);

http.createServer()
  .on('upgrade', proxy.webSocketHandler())
  .listen(8080);
```

And a corresponding client:

```javascript
const avro = require('avsc');
const ws = require('websocket-stream');

const protocol = avro.readProtocol(`
  protocol RemoteLogService {
    void log(enum { INFO, WARNING } level, string message);
  }
`);

const logClient = avro.Service.forProtocol(protocol)
  .createClient({buffering: true, transport: ws('ws://localhost:8080')});

logClient.log('INFO', 'We are now live!');

setTimeout(function () {
  logClient.log('WARNING', 'And soon we will not.');
  logClient.destroyChannels();
}, 1000);
```
