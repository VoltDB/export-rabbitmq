VoltDB RabbitMQ Export Connector
===============

VoltDB export connector for RabbitMQ.

Installation
===============
Before compiling, you need to make sure Java and Ant are installed. You also
need the VoltDB Enterprise kit.

1. Download the [RabbitMQ Java client](http://www.rabbitmq.com/java-client.html)
and copy the unpacked Jar file into `<voltdb enterprise kit>/lib/extension`.

1. Set the environment variable VOLTDIST to the kit directory `voltdb-ent-x.y`
and run ant. For example,
```bash
VOLTDIST=../voltdb-ent-7.5 ant
```

If the compilation finished successfully, the final Jar file should be copied
over to `voltdb-ent-7.5/lib/extension`.

You can enable export in your deployment file with target set to `rabbitmq` and
start the server.
