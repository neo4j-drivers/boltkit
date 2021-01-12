# Boltkit

Boltkit is a package of utilities for Neo4j driver authors.
The library exposes a set of command line tools as well as a full Python API for working with Neo4j-compatible clients and servers.


## Installation

Installation is via github and can either run globally or within a _virtualenv_.
Installation makes available a versatile command line tool, called `bolt`.

```
$ python3 -m pip install --user git+https://github.com/neo4j-drivers/boltkit@x.y#egg=boltkit
```
where `x` and `y` are replaced with major and minor versions respectively.

```
$ bolt --help
Usage: bolt [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  client  Run a Bolt client
  dist    List available Neo4j releases
  get     Download Neo4j
  proxy   Run a Bolt proxy server
  server  Run a Neo4j cluster or standalone server.
  stub    Run a Bolt stub server
```


## Command Overview

### `bolt client` 

The `bolt client` command allows execution of a Cypher query against a Neo4j service.

Synopsis:
```
$ bolt client --help
Usage: bolt client [OPTIONS] [CYPHER]...

  Run a Bolt client

Options:
  -a, --auth AUTH
  -b, --bolt-version INTEGER
  -s, --server-addr ADDR
  -t, --transaction
  -v, --verbose
  --help                      Show this message and exit.
```

Example:
```
$ bolt client "UNWIND range(1, 10) AS n RETURN n"
```


### `bolt dist`

TODO


### `bolt get`

TODO


### `bolt proxy`

TODO


### `bolt server`

Run a Neo4j cluster or standalone server in one or more local Docker containers.

Synopsis:
```
$ bolt server --help
Usage: bolt server [OPTIONS] [COMMAND]...

  Run a Neo4j cluster or standalone server in one or more local Docker
  containers.

  If an additional COMMAND is supplied, this will be executed after startup,
  with a shutdown occurring immediately afterwards. If no COMMAND is
  supplied, the service will remain available until manually shutdown by
  Ctrl+C.

  A couple of environment variables will also be made available to any
  COMMAND passed. These are:

  - BOLT_SERVER_ADDR
  - NEO4J_AUTH

Options:
  -a, --auth AUTH           Credentials with which to bootstrap the service.
                            These must be specified as a 'user:password' pair
                            and may alternatively be supplied via the
                            NEO4J_AUTH environment variable. These credentials
                            will also be exported to any COMMAND executed
                            during the service run.
  -B, --bolt-port INTEGER   A port number (standalone) or base port number
                            (cluster) for Bolt traffic.
  -c, --n-cores INTEGER     If specified, a cluster with this many cores will
                            be created. If omitted, a standalone service will
                            be created instead. See also -r for specifying the
                            number of read replicas.
  -H, --http-port INTEGER   A port number (standalone) or base port number
                            (cluster) for HTTP traffic.
  -i, --image TEXT          The Docker image tag to use for building
                            containers. The repository can also be included,
                            but will default to 'neo4j'. Note that a Neo4j
                            Enterprise Edition image is required for building
                            clusters.
  -n, --name TEXT           A Docker network name to which all servers will be
                            attached. If omitted, an auto-generated name will
                            be used.
  -r, --n-replicas INTEGER  The number of read replicas to include within the
                            cluster. This option will only take effect if -c
                            is also used.
  -v, --verbose             Show more detail about the startup and shutdown
                            process.
  --help                    Show this message and exit.
```

### `bolt stub` 

The stub Bolt server can be used as a testing resource for client software.
Scripts can be created against which unit tests can be run without the need for a full Neo4j server.

Synopsis:
```
$ bolt stub --help
Usage: bolt stub [OPTIONS] SCRIPT

  Run a Bolt stub server.

  The stub server process listens for an incoming client connection and will
  attempt to play through a pre-scripted exchange with that client. Any
  deviation from that script will result in a non-zero exit code. This
  utility is primarily useful for Bolt client integration testing.

Options:
  -l, --listen-addr ADDR  The address on which to listen for incoming
                          connections in INTERFACE:PORT format, where
                          INTERFACE may be omitted for 'localhost'. If
                          completely omitted, this defaults to ':17687'. The
                          BOLT_LISTEN_ADDR environment variable may be used as
                          an alternative to this option.
  -t, --timeout FLOAT     The number of seconds for which the stub server will
                          wait for an incoming connection before automatically
                          terminating. If unspecified, the server will wait
                          indefinitely.
  -v, --verbose           Show more detail about the client-server exchange.
  --help                  Show this message and exit.
```

A server script describes a conversation between client and server in terms of the messages
exchanged. An example can be seen in the [`test/scripts/count.bolt`](test/scripts/count.bolt) file.

When the server receives a client message, it will attempt to match that against the next client message in the script; if found, this line of the script will be consumed.
Then, any server messages that follow will also be consumed and sent back.
When the client closes its connection, the server will shut down.
If any script lines remain, the server will exit with an error status; if none remain it will exit successfully.
After 30 seconds of inactivity, the server will time out and shut down with an error status.

#### Scripting 

Scripts generally consist of alternating client (`C:`) and server (`S:`) messages.
Each message line contains the message name followed by its fields, in JSON format.

Some messages, such as `RESET`, can be automatically (successfully) consumed if they are not relevant to the current test.
For this use a script line such as `!: AUTO RESET`.

An example:
```
!: BOLT 3
!: AUTO HELLO
!: AUTO RESET

C: RUN "RETURN {x}" {"x": 1} {}
   PULL_ALL
S: SUCCESS {"fields": ["x"]}
   RECORD [1]
   SUCCESS {}
```
