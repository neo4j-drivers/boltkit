# Boltkit

Boltkit is a collection of tools and resources for Neo4j 3.0+ driver authors.


## Installation

```
pip install boltkit
```


## Demo Driver [WORK IN PROGRESS]

Resource: [`boltkit/driver.py`](boltkit/driver.py)

This file contains both a fully-working Neo4j driver as well as a step-by-step tutorial for how to implement a driver in any language.
To view the code and tutorial in a terminal, use:

```
less $(python -c "from boltkit import driver; print(driver.__file__)")
```


## Statement Runner [WORK IN PROGRESS]

Resource: [`boltkit/runner.py`](boltkit/runner.py)

Example:
```
boltrun "UNWIND range(1, 10) AS n RETURN n"
```


## Stub Bolt Server [WORK IN PROGRESS]

Resource: [`boltkit/server.py`](boltkit/server.py)

The stub Bolt server can be used as a testing resource for client software.
Scripts can be created against which unit tests can be run without the need for a full Neo4j server.

A server script describes a conversation between client and server in terms of the messages
exchanged. An example can be seen in the [`test/scripts/count.script`](test/scripts/count.script) file.

When the server receives a client message, it will attempt to match that against the next client message in the script; if found, this line of the script will be consumed.
Then, any server messages that follow will also be consumed and sent back.
When the client closes its connection, the server will shut down.
If any script lines remain, the server will exit with an error status; if none remain it will exit successfully.

Some messages, such as `INIT` and `RESET`, can be automatically (successfully) consumed if they are not relevant to the current test.

### Example Usage

To run a stub server script:
```
boltstub 7687 test/scripts/count.script
```

To run a Cypher command against the stub server:
```
boltrun "UNWIND range(1, 10) AS n RETURN n"
```
