###Cloudera Impala JDBC Example

This example shows how to build and run a maven-based project that executes SQL queries on Cloudera Impala using JDBC and then populates a Neo4J database based on the results of the query. 
Cloudera Impala is a native Massive Parallel Processing (MPP) query engine which enables users to perform interactive analysis of data stored in HBase or HDFS. 

Here are links to more information on Cloudera Impala:

- [Cloudera Enterprise RTQ](http://www.cloudera.com/content/cloudera/en/products/cloudera-enterprise-core/cloudera-enterprise-RTQ.html) 

- [Cloudera Impala Documentation](http://www.cloudera.com/content/support/en/documentation/cloudera-impala/cloudera-impala-documentation-v1-latest.html)

- [Cloudera Impala JDBC Documentation](http://www.cloudera.com/content/cloudera-content/cloudera-docs/Impala/latest/Installing-and-Using-Impala/ciiu_impala_jdbc.html)

- [Impala-User Google Group](https://groups.google.com/a/cloudera.org/forum/?fromgroups#!forum/impala-user)

Neo4J is the world's leading graph database.

Here are links to more information on Neo4J:

- [Neo4J](http://www.neo4j.org)

- [Neo4J Learn](http://www.neo4j.org/learn)

- [Neo4J Google Group](https://groups.google.com/forum/#!forum.neo4j)

To use the Cloudera Impala JDBC driver in your own maven-based project you can copy the `<dependency>` and `<repository>` elements from this project's pom to your own instead of manually downloading the JDBC driver jars.




####Dependencies
To build the project you must have Maven 2.x or higher installed.  Maven info is [here](http://maven.apache.org).


To run the project you must have access to a Hadoop cluster running Cloudera Impala with at least one populated table defined in the Hive Metastore.
Neo4J must also be installed. You can download [Neo4J] from (http://www.neo4j.org/download).

####Configure the example
To configure the example you must:

- Select or create the table(s) to query against.
- Set the query and impalad host in the example source file

These steps are described in more detail below.



#####Select or create the table(s) to run the example with
For this example I created my own table and populated it. 

#####Set the query and impalad host
Edit these two setting in the ImpalaNeo4JImporter.java source file:

- Set the SQL Statement

`private static final String SQL_STATEMENT = "SELECT * from organizations limit 10";`
	
- Set the host for the impalad you want to connect to: 

`private static final String IMPALAD_HOST = "MyImpaladHost";`


####Building the project
To build the project, run the command:

`mvn clean compile`

from the root of the project directory.   There is a build.sh script for your convenience.

