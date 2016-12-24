# Verdon [![NPM version](https://img.shields.io/npm/v/verdon.svg)](https://www.npmjs.com/package/verdon)

Avro RPC command line interface.

## Features

+ Discover remote protocols:

  ```bash
  $ verdon info --json http://ping.avro.server:8117
  {"protocol": "Ping","messages":{"ping":{"request":[],"response":"string"}}}
  ```

+ Emit RPC calls:

  ```bash
  $ verdon call http://echo.avro.server:8117 toUpperCase '{"str":"hi"}'
  "HI"
  ```

+ Setting up quick RPC servers:

  ```bash
  $ verdon serve http://test.avro.server:8117 ./Test.avdl
  ```
