Queueing Demo
====================

NOTE - this example requires DataStax Enterprise > 4.5.0 or Cassandra > 2.0.5 and the cassandra-driver-core version > 2.0.2

## Scenario

This demo shows how to create a queueing system in DataStax Enterprise or with leveled compaction in Cassandara. This demo utilises the in-memory feature in DSE to provide a set amount of space for the queue on the heap. 

The queueing system is based around a circular buffer and is currently single threaded only. There is 2 separate main classes, the Writer and Reader. The default settings are that there are 1000 locations in the buffer. The max no of jobs in the queue at any time is 500.
So if the Writer has written 500 more jobs than the reader has read, the writer will be blocked until the reader finishes the current job. Similarly, if the reader catches up with the writer, the reader will wait until some more jobs have been written.

It's a simple system as this point but I may work on a most advanced version with multi-threading and LWT.  

## Schema Setupp
Note : This will drop the keyspace "datastax_queueing_demo" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

NOTE : to do this with Cassandra rather than DSE, use the following cql file to create the table 

	src/main/resources/cql/create_schema_levelled.cql

To run the writer

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.queueing.Writer"

To run the reader

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.queueing.Reader"
		
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    
    
