VoltDB RabbitMQ Export Connector
===============

VoltDB export connector for RabbitMQ.

Installation
===============
Before compiling, you need to make sure Java and Ant are installed. You also
need the VoltDB Enterprise kit.

1. Download the [RabbitMQ Java client](http://www.rabbitmq.com/java-client.html)
and copy the unpacked Jar file into `voltdb-ent-4.3/lib/extension`.

1. Set the environment variable VOLTDIST to the kit directory `voltdb-ent-4.3`
and run ant. For example,
```bash
VOLTDIST=../voltdb-ent-4.3 ant
```

If the compilation finished successfully, the final Jar file should be copied
over to `voltdb-ent-4.3/lib/extension`.

You can enable export in your deployment file with target set to `rabbitmq` and
start the server.

Testing
===============
To run the Junit tests, you need both the _voltdb_ repository and the _pro_
repository.

1. Build the Enterprise Edition in `voltdb`.

1. Run the tests with the following command
```bash
VOLTDIST=../voltdb ant junit
```
