# LongDB Datastore API.

Datastore API provides a thin layer over several Databases. This API allows the client to use different database engines, while keeping the same API. Following database engines are supported by this API currently :

* [Apache Cassandra Database](http://cassandra.apache.org/) in Client/Server mode - Uses Hector Client API
* [Apache Cassandra Database Embedded Mode](http://wiki.apache.org/cassandra/Embedding) -  This implementation directly interacts with Cassandra DB engine running in same JVM bypassing the socket communication. It uses the APIs available in org.apache.cassandra.thrift.CassandraServer class. 
* [VoltDB Community Edition](http://community.voltdb.com/) Client/Server mode.
* [H2 Database](http://www.h2database.com/html/main.html) Embedded mode. 
* [BerkeleyDB](http://www.oracle.com/technetwork/products/berkeleydb/overview/index.html) Embedded mode.
* [LevelDB Java Implementation](https://github.com/dain/leveldb) Embedded mode - which is based on [LevelDB](http://code.google.com/p/leveldb/) 

## API Features

The API follows Cassandra like Data Model which is described briefly as follows :

* The database can have several 'Tables' - Tables can be created/dropped dynamically using the (API except VoltDB implementation which does not support Tables creation dynamically)
* A Table contains Rows, A row can be accessed using a key called rowID which is of 'long' java type..
* A Row can have multiple Columns, the columns are dynamic, and number of columns, and column names will differ for each row, A column can be accessed using columnID which is also of 'long' java type.
* The values stored in columns are of byte array type, in order to store BLOB data.
* The API also provides methods to traverse through row and columns using Iterators.
* Range operations can be performed on columnID's, e.g. Get/Remove all columns from range A to B.
* The Project also includes :
	* An in-memory database implementation for Testing Purpose. 	
	* Benchmark Test script to compare performance for different Database engines.
	* DBTool can be used as a command line tool to export/Import between database and files or betweend the two different database instances.

### Motivation
Our main motivation to develop this API, and its various implementations is to cater to the requirement where common set of business logic needs to run on Client and Server both. We wanted an API that can be used both on the client machine in embedded mode, and on the Server, A Simple lightweight API that works on different databases so that we can have a choice of Databases, both on the client and the server.

## Instructions

### Prerequisites 

* [JDK-7] (http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Maven 3](http://maven.apache.org/download.html)

Note : 'PB_ROOT' mentioned in the instructions below referes to the directory where this proect source code was extracted.  

### Build Procedure

* Set project home as 'PB_ROOT' environment variable.
	
		export PB_ROOT=/usr/local/workspace/Blockwithme/Datastore

* Increase JVM memory while running maven. 

		export MAVEN_OPTS=-Xmx1024m

* To build the Project :
	
		mvn clean install 

### Java docs generation
	
		mvn javadoc:javadoc 

### Using the API

#### For Cassandra Server Mode
(This API is tested with apache-cassandra-1.0.8)

* Cassandra Server Installation :
	* Follow Cassandra [installation instructions](http://wiki.apache.org/cassandra/GettingStarted) to install Cassandra, 
	* Copy PB_ROOT/CassandraServerSide/target/CassandraServerSide-XXXX.jar to CASSANDRA_HOME/lib folder.
	* Restart Cassandra and make sure that Cassandra cluster is up and running without errors.

* Client API side configuration  
	* Edit 'CassandraConfig.properties' file present in PB_ROOT/DatastoreCassandraImpl/conf. 

* Maven Dependencies - make sure following Maven dependencies are available in your project to use the Cassandra implementaion of this API

		<dependencies>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>API</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>BaseImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>CassandraServerSide</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>CassandraImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.carrotsearch</groupId>
				<artifactId>hppc</artifactId>
			</dependency>
			<dependency>
				<groupId>org.apache.cassandra</groupId>
				<artifactId>cassandra-all</artifactId>
			</dependency>
			<dependency>
				<groupId>me.prettyprint</groupId>
				<artifactId>hector-core</artifactId>
			</dependency>
			<dependency>
				<groupId>commons-configuration</groupId>
				<artifactId>commons-configuration</artifactId>
			</dependency>
		</dependencies>

* To run Benchmark Tests for Cassandra Server mode run following maven command

	mvn --projects BenchmarkTests -Dtest=HectorCassandraBenchmark test

#### For Cassandra Embedded Mode 
(This API is tested with apache-cassandra-1.0.8)

* No Database installation is needed for Embedded mode.
* Edit 'CassandraConfig.properties' file available present in PB_ROOT/CassandraEmbedded/conf, make sure 'CassandraBackend.isEmbedded' property is set to true.
* Edit cassandra.yaml, make sure that Cluster name in both the configuration file is same.
* Make sure following Maven dependencies are available in your project to use the Cassandra implementaion of this API 

		<dependencies>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>API</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>BaseImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>CassandraServerSide</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>CassandraImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.carrotsearch</groupId>
				<artifactId>hppc</artifactId>
			</dependency>
			<dependency>
				<groupId>org.slf4j</groupId>
				<artifactId>slf4j-api</artifactId>
			</dependency>
			<dependency>
				<groupId>org.apache.cassandra</groupId>
				<artifactId>cassandra-all</artifactId>
			</dependency>
			<dependency>
				<groupId>commons-configuration</groupId>
				<artifactId>commons-configuration</artifactId>
			</dependency>
			<dependency>
				<groupId>log4j</groupId>
				<artifactId>log4j</artifactId>
			</dependency>
		</dependencies>

* To run Benchmark Tests for Cassandra Embedded mode run following maven command

	mvn --projects BenchmarkTests -Dtest=EmbeddedCassandraBenchmark test

#### VoltDB 

* VoltDB Server Installation :
	* Follow VoltDB [installation instructions](http://community.voltdb.com/docs/UsingVoltDB/installDist) to install VoltDB Server 
	* Copy VoltDBServer-XXXX.jar to target server machine in VOLT_DB_HOME/lib folder.
	* On each target Server machine create a new folder parallel to VOLT_DB_HOME and copy following files inside it :
		* PB_ROOT/VoltDBServer/src/project.xml
		* PB_ROOT/VoltDBServer/src/schema.sql
		* PB_ROOT/VoltDBServer/src/deployment.xml
		* PB_ROOT/VoltDBServer/scripts/voltserver
		* PB_ROOT/VoltDBServer/target/VoltDBServer-XXXX-catalog.jar
	* Edit PB_ROOT/VoltDBServer/scripts/voltserver and set VOLT_DB_HOME environment variable.
       	* Edit deployment.xml with details like hostcount etc. For single node cluster leave the this unchanged. 
        * To start the server run 'voltserver' script and make sure that the server starts without errors.

* Client API side configuration
	* Edit 'VoltConfig.properties' file present in PB_ROOT/VoltDBImpl/conf. 


* Make sure following Maven dependencies are available in your project to use VoltDB implementation of the API.

		<dependencies>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>API</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>BaseImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.carrotsearch</groupId>
				<artifactId>hppc</artifactId>
			</dependency>
			<dependency>
				<groupId>org.slf4j</groupId>
				<artifactId>slf4j-api</artifactId>
			</dependency>
			<dependency>
				<groupId>org.voltdb</groupId>
				<artifactId>voltdbclient</artifactId>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>VoltDBImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
		</dependencies>


* To run Benchmark Tests for VoltDB implementation, run following maven command :
	
		mvn --projects BenchmarkTests -Dtest=VoltDBBenchmark test

#### H2 Database Embedded mode

* Edit 'H2Config.properties' file available present in PB_ROOT/H2Impl/conf.
* Make sure following Maven dependencies are available in your project to use the H2 Database implementaion of the API. 

		<dependencies>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>API</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>BaseImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.carrotsearch</groupId>
				<artifactId>hppc</artifactId>
			</dependency>
			<dependency>
				<groupId>org.slf4j</groupId>
				<artifactId>slf4j-api</artifactId>
			</dependency>
			<dependency>
				<groupId>com.h2database</groupId>
				<artifactId>h2</artifactId>
			</dependency>
		</dependencies>

* To run Benchmark Tests for H2 Database mode run following maven command :
	
		mvn --projects BenchmarkTests -Dtest=H2DBBenchmark test

#### BerkeleyDB

* Edit 'BDBConfig.properties' file available present in PB_ROOT/BerkeleyDBImpl/conf.
* Make sure following Maven dependencies are available in your project to use the BerkeleyDB implementaion of the API. 
	
		<dependencies>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>API</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>BaseImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>BerkeleyDBImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.carrotsearch</groupId>
				<artifactId>hppc</artifactId>
			</dependency>
			<dependency>
				<groupId>commons-io</groupId>
				<artifactId>commons-io</artifactId>
			</dependency>
			<dependency>
				<groupId>com.sleepycat</groupId>
				<artifactId>je</artifactId>
			</dependency>
		</dependencies>

* To run Benchmark tests for BerkeleyDB run following maven command:
	
	mvn --projects BenchmarkTests -Dtest=BDBBenchmark test

#### LevelDB Java Implementation

* Edit 'LevelDBConfig.properties' file available present in PB_ROOT/LevelDBJavaImpl/conf.
* Make sure following Maven dependencies are available in your project to use the LevelDB implementaion of this API. 
	
		<dependencies>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>API</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>BaseImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.blockwithme</groupId>
				<artifactId>LevelDBJavaImpl</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
			<dependency>
				<groupId>com.carrotsearch</groupId>
				<artifactId>hppc</artifactId>
			</dependency>
			<dependency>
				<groupId>org.iq80.leveldb</groupId>
				<artifactId>leveldb</artifactId>
			</dependency>
			<dependency>
				<groupId>commons-io</groupId>
				<artifactId>commons-io</artifactId>
			</dependency>
			<dependency>
				<groupId>commons-lang</groupId>
				<artifactId>commons-lang</artifactId>
			</dependency>
		</dependencies>

* To run Benchmark tests for LevelDB run following maven command:
	
		mvn --projects BenchmarkTests -Dtest=JavaLevelDBBenchmark test

### DBTool

* Use scripts under to PB_ROOT/DBTools/script to run DBTool, use help command to know more details about DBTool

		DBTool -help
