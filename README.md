Paging Demo
====================

NOTE - this example requires apache cassandra version > 2.0.8 and the cassandra-driver-core version > 2.0.2

## Scenario


## Schema Setup
Note : This will drop the keyspace "datastax_paging_demo" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To run the insert

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.queueing.Writer"

To run the reader

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.queueing.Reader"
		
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    
    
