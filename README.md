# Boltkit

Boltkit is a collection of tools and resources for Neo4j 3.0+ driver authors.


## Installation

The package can either be installed globally or within a *virtualenv*.
Installation makes available several command line tools, the names of which all start with `bolt`.
```
pip install boltkit
```


## Demo Driver

- Source: [`boltkit/driver.py`](boltkit/driver.py)

This file contains both a fully-working Neo4j driver as well as a step-by-step tutorial for how to implement a driver in any language.
To view the code and tutorial in a terminal, use:

```
less $(python -c "from boltkit import driver; print(driver.__file__)")
```


## Statement Runner

- Command: `boltrun <statement>`
- Source: [`boltkit/runner.py`](boltkit/runner.py)

Example:
```
boltrun "UNWIND range(1, 10) AS n RETURN n"
```


## Stub Bolt Server

- Command: `boltstub <port> <script>`
- Source: [`boltkit/server.py`](boltkit/server.py)

The stub Bolt server can be used as a testing resource for client software.
Scripts can be created against which unit tests can be run without the need for a full Neo4j server.

A server script describes a conversation between client and server in terms of the messages
exchanged. An example can be seen in the [`test/scripts/count.bolt`](test/scripts/count.bolt) file.

When the server receives a client message, it will attempt to match that against the next client message in the script; if found, this line of the script will be consumed.
Then, any server messages that follow will also be consumed and sent back.
When the client closes its connection, the server will shut down.
If any script lines remain, the server will exit with an error status; if none remain it will exit successfully.
After 30 seconds of inactivity, the server will time out and shut down with an error status.

### Scripting

Scripts generally consist of alternating client (`C:`) and server (`S:`) messages.
Each message line contains the message name followed by its fields, in JSON format.

Some messages, such as `INIT` and `RESET`, can be automatically (successfully) consumed if they are not relevant to the current test.
For this use a script line such as `!: AUTO INIT`.

An example:
```
!: AUTO INIT
!: AUTO RESET

C: RUN "RETURN {x}" {"x": 1}
   PULL_ALL
S: SUCCESS {"fields": ["x"]}
   RECORD [1]
   SUCCESS {}
```


### Command Line Usage

To run a stub server script:
```
boltstub 7687 test/scripts/count.bolt
```

To run a Cypher command against the stub server:
```
boltrun "UNWIND range(1, 10) AS n RETURN n"
```

### Java Test Usage

The stub server can be used from any environment from which command line tools can be executed.
To use from Java, first construct a wrapper for the server:
```java
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class StubServer
{
    // This may be thrown if the driver has not been closed properly
    public static class ForceKilled extends Exception {}

    private Process process = null;

    private StubServer( String script, int port ) throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        // This assumes the `boltstub` command is available on the path
        command.addAll( singletonList( "boltstub" ) );
        command.addAll( asList( Integer.toString( port ), script ) );
        ProcessBuilder server = new ProcessBuilder().inheritIO().command( command );
        process = server.start();
        sleep( 500 );  // might take a moment for the socket to start listening
    }

    public static StubServer start( String script, int port ) throws IOException, InterruptedException
    {
        return new StubServer( script, port );
    }

    public int exitStatus() throws InterruptedException, ForceKilled
    {
        sleep( 500 );  // wait for a moment to allow disconnection to occur
        try
        {
            return process.exitValue();
        }
        catch ( IllegalThreadStateException ex )
        {
            // not exited yet
            process.destroy();
            process.waitFor();
            throw new ForceKilled();
        }
    }

}
```

Then, assuming you have a valid script available, create a test to use the stub:
```java
@Test
public void shouldBeAbleRunCypher() throws StubServer.ForceKilled, InterruptedException, IOException
{
    // Given
    StubServer server = StubServer.start( "/path/to/resources/return_x.bolt", 7687 );
    URI uri = URI.create( "bolt://localhost:7687" );
    int x;

    // When
    try ( Driver driver = GraphDatabase.driver( uri ) )
    {
        try ( Session session = driver.session() )
        {
            Record record = session.run( "RETURN {x}", parameters( "x", 1 ) ).single();
            x = record.get( 0 ).asInt();
        }
    }

    // Then
    assertThat( x, equalTo( 1 ) );

    // Finally
    assertThat( server.exitStatus(), equalTo( 0 ) );
}
```