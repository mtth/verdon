# Verdon [![NPM version](https://img.shields.io/npm/v/verdon.svg)](https://www.npmjs.com/package/verdon) [![Build status](https://travis-ci.org/mtth/verdon.svg?branch=master)](https://travis-ci.org/mtth/verdon)

Avro services CLI.

## Features

+ Assemble service protocols from their IDL specification:

  ```bash
  $ verdon assemble LinkService.avdl
  ```

+ Discover remote services:

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

Run `verdon --help` to view the full list of commands and options for each.
