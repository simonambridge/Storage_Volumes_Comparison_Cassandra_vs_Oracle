<h1>Migrating A Large ASCII Data Set From CSV To Oracle 12c To Cassandra 3.0 - A look at storage impact</h1>
The objective of this exercise is to demonstrate how the migration of data from CSV to Oracle to Cassandra can change the required underlying storage volume of the data. How do data volumes change between pure ASCII source records versus records stored in tables in an Oracle database versus records stored in tables in a DSE/Cassandra database?

Here is the high-level plan:
<ul>
<li>
Start out with a 1.4 GB CSV data file containing 6.1 million crime records from the Chicago Police Department for the period 2001-2016
</li>
<li>
Define this file as an external data file in Oracle 12c 
</li>
<li>
Read the external data file into a physical Oracle database table 
</li>
<li>
Use the Spark DataFrame capability introduced in Apache Spark 1.3 to load data from tables in the Oracle database via Oracle's JDBC thin driver
</li>
<li>
Save the data in the dataframe to Cassandra
</li>
<li>
Compare the volumes used by the data in Oracle versus DSE/Cassandra
</li>
</ul>


<h2>Pre-requisites</h2>
<h3> DataStax Enterprise (release 5.0.2 at time of publication)</h3>
You'll need a working installation of DataStax Enterprise.

- Ubuntu/Debian - https://docs.datastax.com/en/datastax_enterprise/5.0/datastax_enterprise/install/installDEBdse.html
- Red Hat/Fedora/CentOS/Oracle Linux - https://docs.datastax.com/en/datastax_enterprise/5.0/datastax_enterprise/install/installRHELdse.html

To setup your environment, you'll also need the following resources:

- Python 2.7
- Java 8
- For Red Hat, CentOS and Fedora, install EPEL (Extra Packages for Enterprise Linux).
- An Oracle database:
 - In my example the database name is orcl
 - A running Oracle tns listener. In my example I'm using the default port of 1521.
 - You are able to make a tns connection to the Oracle database e.g. <b>"sqlplus user/password@service_name"</b>.
- The Oracle thin JDBC driver. You can download the ojdbc JAR file from:
http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html
I've used ojdbc7.jar which is certified for use with both JDK7 and JDK8
In my 12c Oracle VM Firefox this downloaded to /app/oracle/downloads/ so you'll see the path referenced in the instructions below.


As we will connect from Spark, using the Oracle jdbc driver, to the "orcl" database on TNS port 1521, all these components must be working correctly.
<br>

Now on to installing DataStax Enterprise and playing with some data!
<p>
<br>
<H1>Set Up DataStax Components</H1>

Installation instructions for DSE are provided at the top of this doc. I'll show the instructions for Red Hat/CentOS/Fedora here. I'm using an Oracle Enterprise Linux VirtualBox instance.
<h2>Add The DataStax Repo</H2>
As root create ```/etc/yum.repos.d/datastax.repo```
<pre># vi /etc/yum.repos.d/datastax.repo
</pre>

Paste in these lines:
<pre>
[datastax] 
name = DataStax Repo for DataStax Enterprise
baseurl=https://datastaxrepo_gmail.com:utJVKEg4lKeaWTX@rpm.datastax.com/enterprise
enabled=1
gpgcheck=0
</pre>

<h2>Import The DataStax Repo Key</H2>
<pre>
rpm --import http://rpm.datastax.com/rpm/repo_key 
</pre>


<h2>Install DSE Components</H2>
<h3>DSE Platform</h3>
<pre>
# yum install dse-full-5.0.1-1
</pre>

<h3>DataStax OpsCenter</h3>
<pre>
# yum install opscenter --> 6.0.2.1
</pre>

<h3>DataStax OpsCenter Agent</h3>
<pre>
# yum install datastax-agent --> 6.0.2.1
</pre>
<br>

<h2>Enable Search & Analytics</h2>

We want to use Search (Solr) and Analytics (Spark) so we need to delete the default datacentre and restart the cluster (if its already running) in SearchAnalytics mode.
<br>
Stop the service if it's running.
<pre>
#  service dse stop
Stopping DSE daemon : dse                                  [  OK  ]
</pre>
<br>
Enable Solr and Spark by changing the flag from "0" to "1" in:
<pre>
# vi /etc/default/dse
</pre>
e.g.:
<pre>
# Start the node in DSE Search mode
SOLR_ENABLED=1

# Start the node in Spark mode
SPARK_ENABLED=1
</pre>
<br>
Delete the old (Cassandra-only) datacentre databases if they exist:
<pre>
# rm -rf /var/lib/cassandra/data/*
# rm -rf /var/lib/cassandra/saved_caches/*
# rm -rf /var/lib/cassandra/commitlog/*
# rm -rf /var/lib/cassandra/hints/*
</pre>

Remove the old system.log if it exists:
<pre>
# rm -rf /var/log/cassandra/system.log 
</pre>

Now restart DSE:
<pre>
$ sudo service DSE restart
</pre>
<br>

After a few minutes use nodetool to check that all is up and running (check for "UN" next to the IP address):
<pre>
$ nodetool status
Datacenter: SearchAnalytics
===========================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load       Owns    Host ID                               Token                                    Rack
UN  127.0.0.1  346.89 KB  ?       8e6fa3db-9018-47f0-96df-8c78067fddaa  6840808785095143619                      rack1

Note: Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless
</pre>
You should also check that you can log into cqlsh:
<pre>
$ cqlsh
Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.0.7.1159 | DSE 5.0.1 | CQL spec 3.4.0 | Native protocol v4]
Use HELP for help.
cqlsh> 
</pre>
Type exit in cqlsh to return to the shell prompt.
<br>

<H2>Identify Spark Master</h2>
Using the new DSE 5.0 command format for the dse tool we get the address of the Spark Master in our cluster. 
As we are using a single node for our cluster it will be no surprise that the Spark Master is also on our single node!
<pre>
$ dse client-tool spark master-address
spark://127.0.0.1:7077
</pre> 
OK, let's go get some data.<p>
<br>

<H1>Load CSV Data Into Oracle</h1>
Our source file is an extract of the Chicago Crime Database 2001 to 2016, described here: http://catalog.data.gov/dataset/crimes-2001-to-present-398a4 that can be found here:https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD
<br>

The dataset is structured with the following fields:

- ID             
- Case Number	   
- Date	
- Block	
- UCR	
- Primary Type	
- Description	
- Location Description	
- Arrest	
- Domestic	
- Beat	
- District	
- Ward	
- Community Area	
- FBI Code	
- X Coordinate	
- Y Coordinate	
- Year	
- Updated On	
- Latitude	
- Longitude	
- Location

The records in the data file look like this, with a header record:
<pre>
ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location

5784095,HN594666,09/17/2007 05:10:00 PM,021XX W WARREN BLVD,0486,BATTERY,DOMESTIC BATTERY SIMPLE,CHA APARTMENT,false,true,1332,012,2,28,08B,1162213,1900335,2007,04/15/2016 08:55:02 AM,41.882182894,-87.679812546,"(41.882182894, -87.679812546)"
5784096,HN594881,09/17/2007 06:35:00 PM,013XX S RACINE AVE,0460,BATTERY,SIMPLE,STREET,false,false,1231,012,2,28,08B,1168579,1894058,2007,04/15/2016 08:55:02 AM,41.864822916,-87.656618405,"(41.864822916, -87.656618405)"
</pre>

We will have to build a table in Oracle for the first import of the data. When we have the data in Oracle we can assess the volume of space occupied by database tables.
We then migrate the data to Cassandra from Oracle using Spark and see how much space is occupied in Cassandra. <p>

Wordcount tells me there are just over 6 million records (about 1.4 GB CSV file):
<pre>
$ cat Crimes_-_2001_to_present.csv | wc
 6156888 69665305 1449194734
</pre>
<br>
<h2>Create An Oracle User</h2>
We need an owner of the tables we're going to create. We'll create a user called BULK_LOAD.
To do this we need to log into Oracle SQLPlus as a privileged user (SYS or SYSTEM) to get to the SQL prompt
<li>depending on your privileges you may be asked for a password when you log in as SYS or SYSTEM</li>
<li>If you're already logged into SQLPlus as a non-system user, just type "connect / as sysdba" at the SQL> prompt</li>
<p>

Log in to SQLPlus:
<pre lang="sql">
$ sqlplus / as sysdba
 
Connected.
</pre>

Run this command to allow local User ID's to be created:
<pre lang="sql">
SQL> alter session set "_ORACLE_SCRIPT"=true; 
</pre>

Create the user BULK_LOAD and set the password to be the same as the user name:
<pre lang="sql">
SQL> create user bulk_load identified by bulk_load;

User created.
</pre>

Grant everything to BULK_LOAD (the old-fashioned way):
<pre lang="sql">
SQL> grant connect, resource, dba to bulk_load;

Grant succeeded.
</pre>

Set default user and temporary tablespaces:
<pre lang="sql">
SQL> alter user bulk_load default tablespace "USERS";

User altered.

SQL> alter user bulk_load temporary tablespace "TEMP";

User altered.
</pre>
<br>

<h2>Define An External File Directory</h2>
This is how Oracle reads and manages flat files, allowing them to be manipulated as virtual tables. Great for loading data!

<pre lang="sql">
SQL> create or replace directory xtern_data_dir as '/app/oracle/downloads';

Directory created.
</pre>

Grant access to our BULK_LOAD user:
<pre lang="sql">
SQL> grant read,write on directory xtern_data_dir to bulk_load;

Grant succeeded.
</pre>


<h2>Define An External File Table</h2>
This is how Oracle reads a text file and manages it as an external table.
Connect as our user:
<pre lang="sql">
SQL> connect bulk_load/bulk_load;
Connected.
</pre>
Create the 'virtual' table based on the source data file.

>The data is rather inconsistent in terms of enforcing format and type. I don't want to have to spend a lifetime data-cleansing all 6.1 m records before starting this project so the CSV load into Oracle is in pure text format.

In the first test we will see what happens to data when it is transferred into Oracle and then to DSE/Cassandra, all in text format. This will make an interesting baseline comparison.<p>
The second part of the exercise will be to build a more representative test by storing the data using a wider variety of datatypes, e.g. decimal, binary, date etc.
First of all, we need to create the external table that we defined. What this does is provide Oracle with a template to read the external file:
<pre lang="sql">
drop table xternal_crime_data;
create table xternal_crime_data
(ID             			varchar2(30),
Case_Number				varchar2(30),
Incident_Date				varchar2(30),
Block					varchar2(60),
IUCR					varchar2(30),
Primary_Type				varchar2(60),
Description				varchar2(120),
Location_Description		varchar2(60),
Arrest					varchar2(60),
Domestic				varchar2(60),
Beat					varchar2(30),
District				varchar2(30),
Ward					varchar2(30),
Community_Area				varchar2(30),
FBI_Code				varchar2(10),
X_Coordinate				varchar2(30),
Y_Coordinate				varchar2(30),
Year					varchar2(30),
Updated_On				varchar2(30),
Latitude				varchar2(30),
Longitude				varchar2(30),
Location				varchar2(60)
)
organization external
( default directory xtern_data_dir
access parameters
( records delimited by newline
fields terminated by ',' optionally enclosed by '(' and ')' MISSING FIELD VALUES ARE NULL)
location ('Crimes_-_2001_to_present.csv')  
) REJECT LIMIT UNLIMITED;

Table created.
</pre>

We have to do this next step:
<pre lang="sql">
SQL> alter table xternal_crime_data reject limit unlimited;
</pre>
To avoid this error:
<pre lang="sql">
SQL> select count(*) from xternal_crime_data;
select count(*) from xternal_crime_data
*
ERROR at line 1:
ORA-29913: error in executing ODCIEXTTABLEFETCH callout
ORA-30653: reject limit reached
</pre>

If we describe the 'table' it matches the format of the physical file it's based on:
<pre lang="sql">
SQL> desc xternal_crime_data
 Name					   Null?    Type
 ----------------------------------------- -------- ----------------------------
 ID                                                 VARCHAR2(30)
 CASE_NUMBER					    VARCHAR2(30)
 INCIDENT_DATE					    VARCHAR2(30)
 BLOCK						    VARCHAR2(60)
 IUCR						    VARCHAR2(30)
 PRIMARY_TYPE					    VARCHAR2(60)
 DESCRIPTION					    VARCHAR2(120)
 LOCATION_DESCRIPTION				    VARCHAR2(60)
 ARREST 					    VARCHAR2(60)
 DOMESTIC					    VARCHAR2(60)
 BEAT						    VARCHAR2(30)
 DISTRICT					    VARCHAR2(30)
 WARD						    VARCHAR2(30)
 COMMUNITY_AREA 				    VARCHAR2(30)
 FBI_CODE					    VARCHAR2(10)
 X_COORDINATE					    VARCHAR2(30)
 Y_COORDINATE					    VARCHAR2(30)
 YEAR						    VARCHAR2(30)
 UPDATED_ON					    VARCHAR2(30)
 LATITUDE					    VARCHAR2(30)
 LONGITUDE					    VARCHAR2(30)
 LOCATION					    VARCHAR2(60)
</pre>

So far Oracle hasn't actually looked at the data yet. When we try to read the table the records will be read and validated. 

Check how many records there are in the original (source) external 'table' - just over 6 million:
<pre lang="sql">
SQL> select count(*) from xternal_crime_data;

  COUNT(*)
----------
   6156888
</pre>

You can watch for errors or bad records while the data is being imported using tail or less on these files - log output will be to the default directory that was specified in the create external table statement:
<pre>
$ rm /app/oracle/downloads/*.bad
$ rm /app/oracle/downloads/*.log
</pre>

Nothing happens until you attempt to read data from the table. You can track progress by using tail -f or less to view the log and bad files.

<h2>Create A Table, Physically Load Data Into Oracle</h2>
Now we're going to create a real table in the Oracle database and copy the data in from the external file via the external table definition. 
We can create a physical table in the Oracle database using the "...as select" SQL syntax e.g.:
<pre lang="sql">
SQL> create table crimes as select * from xternal_crime_data;
</pre>

We now have the data in the crimes tables.

The header record is in the table and we don't want it there. Delete it (where ID is not numeric) with the following statement:
<pre lang='sql'>
SQL> delete from crimes where length(trim(translate(ID, ' +-.0123456789', ' '))) is not null;  
</pre>

Let's put a primary key and index on that table:
<pre lang="sql">
SQL> create index crime_id on crimes(id);

Index created.
</pre>

<pre lang="sql">
SQL> alter table crimes add constraint crimes_pk primary key(id);

Table altered.
</pre>

We now have an index on our primary key which (showing as a "NOT NULL" column).
In the real-world, we would probably make Crime ID a unique index.

<pre lang="sql">
SQL> desc crimes
 Name				   Null?    Type
 --------------------- -------- ----------------------------
 ID					   NOT NULL VARCHAR2(30)
 CASE_NUMBER					VARCHAR2(30)
 INCIDENT_DATE					VARCHAR2(30)
 BLOCK						    VARCHAR2(60)
 IUCR						    VARCHAR2(30)
 PRIMARY_TYPE					VARCHAR2(60)
 DESCRIPTION					VARCHAR2(120)
 LOCATION_DESCRIPTION			VARCHAR2(60)
 ARREST 					    VARCHAR2(60)
 DOMESTIC					    VARCHAR2(60)
 BEAT						    VARCHAR2(30)
 DISTRICT					    VARCHAR2(30)
 WARD						    VARCHAR2(30)
 COMMUNITY_AREA 				VARCHAR2(30)
 FBI_CODE					    VARCHAR2(10)
 X_COORDINATE					VARCHAR2(30)
 Y_COORDINATE					VARCHAR2(30)
 YEAR						    VARCHAR2(30)
 UPDATED_ON					    VARCHAR2(30)
 LATITUDE					    VARCHAR2(30)
 LONGITUDE					    VARCHAR2(30)
 LOCATION					    VARCHAR2(60)
</pre>
<br>
<h2>Check Volumes In Oracle</h2>
At this point we have loaded the CSV source data (1.6 GB, 6.1 million rows) into a table in Oracle.
We need to find out - how big is it?

<h3>What Objects Have We Created?</h3>
What objects does bulk_load own - the system dictionary table DBA_TABLES will tell us:
<pre lang="sql">
SQL> connect / as sysdba
</pre>

<pre lang="sql">
SQL> select table_name from dba_tables where owner = 'BULK_LOAD';

TABLE_NAME
--------------------------------------------------------------------------------
CRIMES
XTERNAL_CRIME_DATA
</pre>

<h3>How Much Space Is Oracle Using?</h3>

Run this next query to determine total storage for a user including indexes, blobs etc. 
You will be prompted for the username of the Oracle user that you want to check, in uppercase - in this case it's BULK_LOAD:

<pre lang="sql">
COLUMN TABLE_NAME FORMAT A32
COLUMN OBJECT_NAME FORMAT A32
COLUMN OWNER FORMAT A10

SELECT
   owner, 
   table_name, 
   TRUNC(sum(bytes)/1024/1024) Meg,
   ROUND( ratio_to_report( sum(bytes) ) over () * 100) Percent
FROM
(SELECT segment_name table_name, owner, bytes
 FROM dba_segments
 WHERE segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
 UNION ALL
 SELECT i.table_name, i.owner, s.bytes
 FROM dba_indexes i, dba_segments s
 WHERE s.segment_name = i.index_name
 AND   s.owner = i.owner
 AND   s.segment_type IN ('INDEX', 'INDEX PARTITION', 'INDEX SUBPARTITION')
 UNION ALL
 SELECT l.table_name, l.owner, s.bytes
 FROM dba_lobs l, dba_segments s
 WHERE s.segment_name = l.segment_name
 AND   s.owner = l.owner
 AND   s.segment_type IN ('LOBSEGMENT', 'LOB PARTITION')
 UNION ALL
 SELECT l.table_name, l.owner, s.bytes
 FROM dba_lobs l, dba_segments s
 WHERE s.segment_name = l.index_name
 AND   s.owner = l.owner
 AND   s.segment_type = 'LOBINDEX')
WHERE owner in UPPER('&owner')
GROUP BY table_name, owner
HAVING SUM(bytes)/1024/1024 > 10  /* Ignore really small tables */
ORDER BY SUM(bytes) desc
;
</pre>

<pre>
OWNER	   TABLE_NAME				   MEG	  PERCENT
---------- -------------------------------- ---------- ----------
BULK_LOAD  CRIMES    				  1672	      100
</pre>


So a 1.4GB CSV file has become 1.7GB of storage in Oracle.<p>
<br>
<H1>Copy Data From Oracle To Cassandra</H1>
We now have 1.7GB of data in Oracle. 
The next objective is to see how much space that the 1.7GB of data in Oracle occupies when it is copied to Cassandra. What impact does Cassandra on-disk compression and the new Cassandra 3.0 storage engine  have on those numbers?

So all is looking good on the Oracle side. Now time to turn to Cassandra and Spark. We're going to use Spark to read the data from the Oracle table via JDBC using the Oracle JDBC driver.

<h2>Download Oracle ojdbc7.jar</h2>
We have to add the Oracle JDBC jar file to our Spark classpath so that Spark knows how to talk to the Oracle database.

<br>
<h3>Using The Oracle JDBC Driver</h3>
For this test we only need the jdbc driver file on our single (SparkMaster) node.
In a bigger cluster we would need to distribute it to the slave nodes too.

<h3>Update Executor Path For ojdbc7.jar In spark-defaults.conf</h3>
Add the classpath for the ojdbc7.jar file for the executors (the path for the driver seems be required on the command line at run time as well, see below).
<pre>
# vi /etc/dse/spark/spark-defaults.conf
</pre>
Add the following lines pointing to the location of your ojdbc7.jar file:
<pre>
spark.driver.extraClassPath = /app/oracle/downloads/ojdbc7.jarr
spark.executor.extraClassPath = /app/oracle/downloads/ojdbc7.jar
</pre>

<h2>Restart DSE</h2>
<pre>
$ sudo service dse stop
$ sudo service dse start
</pre>

<h2>Create Cassandra KeySpace</h2>
Before we start pulling data out of Oracle we need somewhere to put it. So we need to create a table in Cassandra.

The first thing that we need to do in Cassandra is create a keyspace to contain the table that will hold the data. I'm using a replication factor of 1 for my data because I have just one node in my development cluster. For most production deployments we recommend a multi-datacenter Active-Active HA setup, across geographical regions where necessary, using NetworkTopologyStrategy with RF=3:

We create the Cassandra table in the CQL shell.
Log into cqlsh 
> If you didn't change the IP defaults in cassandra.yaml then just type 'cqlsh' - if you changed the IP to be the host IP then you may need to supply the hostname e.g. 'cqlsh [hostname]'.

From the cqlsh prompt, create the keyspace:
<pre>
CREATE KEYSPACE IF NOT EXISTS bulk_load WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
USE bulk_load;
</pre>
<h2>Create A Cassandra Table</h2>

The source data had some inconsistencies that made it simpler to load all the data into Oracle as text columns. To make the comparison accurate we'll store the data in Cassandra in text format too.

We'll create a Cassandra table that matches the Oracle columns, with a partition key based on the crime ID.
We're going to turn compression off in this table, again in the interests of making it a valid comparison with Oracle. 

>Compression is on by default in Cassandra 3.0 - however the massive storage engine improvements for Cassandra 3.0 means that even uncompressed data is still significantly smaller than compressed data in Cassandra 2.x. The compression overhead is not an issue but it obviously helps if you don't need compression. Consequently the general recommendation is to disable compression in Cassandra 3.x deployments.

In cqlsh create the crimes table:

<pre lang="sql">
cqlsh:bulk_load> drop table if exists crimes;

create table crimes (
id                      text,
case_number             text,
incident_date           text,
block                   text,
iucr                    text,
primary_type            text,
description             text,
location_description    text,
arrest                  text,
domestic                text,
beat                    text,
district                text,
ward                    text,
community_area          text,
fbi_code                text,
x_coordinate            text,
y_coordinate            text,
year                    text,
updated_on              text,
latitude                text,
longitude               text, 
location                text, 
PRIMARY KEY (id))
WITH compression = { 'sstable_compression' : '' };
</pre>

You can check it's all there in Cassandra:
<pre lang="sql">
cqlsh:bulk_load> desc table crimes;

CREATE TABLE bulk_load.crimes (
    id text PRIMARY KEY,
    arrest text,
    beat text,
    block text,
    case_number text,
    community_area text,
    description text,
    district text,
    domestic text,
    fbi_code text,
    incident_date text,
    iucr text,
    latitude text,
    location text,
    location_description text,
    longitude text,
    primary_type text,
    updated_on text,
    ward text,
    x_coordinate text,
    y_coordinate text,
    year text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'enabled': 'false'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
</pre>

> Note that there are no settings for compression shown for this table - the data will not be compressed.

No we can go back to Spark to start moving the data to our new Cassandra table...

<h2>Start The Spark REPL</h2>
We'll use the Spark REPL (shell) to move the data from Oracle to Cassandra. We can use Spark Dataframes to read from Oracle via JDBC and then write back out to Cassandra. The beauty of processing with Spark is that you can do so much with the data while it's in flight - perfect for migrating and de-normalising relational schemas.

I'm passing to the path to the ojdbc7.jar file on the command line (shouldn't be needed as the driver path is defined in the spark-defaults.conf file now, but it seems not to work without it).

<pre>
$ dse spark --driver-class-path /app/oracle/downloads/ojdbc7.jar -deprecation
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.1
      /_/

Using Scala version 2.10.5 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_77)
Type in expressions to have them evaluated.
Type :help for more information.
Initializing SparkContext with MASTER: spark://127.0.0.1:7077
Created spark context..
Spark context available as sc.
Hive context available as sqlContext. Will be initialized on first use.

scala> 
</pre>
<h3>Import Some UsefulClasses</h3>

Now import some classes we might need:
<pre lang="java">
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._
</pre>

<h3>Load The Data to a DataFrame Via JDBC</h3>
Next, load the data from the Oracle table using JDBC.<p>
For Spark versions below 1.4:
<pre lang="scala">
scala> val crimes = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:bulk_load/bulk_load@localhost:1521/orcl", "dbtable" -> "crime_data"))
</pre>
For Spark 1.4 onwards:
<pre lang="scala">
scala> val crimes = sqlContext.read.format("jdbc").option("url", "jdbc:oracle:thin:bulk_load/bulk_load@localhost:1521/orcl").option("driver", "oracle.jdbc.OracleDriver").option("dbtable", "crimes").load()
</pre>
Response:
<pre lang="scala">
crimes: org.apache.spark.sql.DataFrame = [ID: string, CASE_NUMBER: string, INCIDENT_DATE: string, BLOCK: string, IUCR: string, PRIMARY_TYPE: string, DESCRIPTION: string, LOCATION_DESCRIPTION: string, ARREST: string, DOMESTIC: string, BEAT: string, DISTRICT: string, WARD: string, COMMUNITY_AREA: string, FBI_CODE: string, X_COORDINATE: string, Y_COORDINATE: string, YEAR: string, UPDATED_ON: string, LATITUDE: string, LONGITUDE: string, LOCATION: string]
</pre>

There are some options available that allow you to tune how Spark uses the JDBC driver. The JDBC datasource supports partitionning so that you can specify how Spark will parallelize the load operation from the JDBC source. By default a JDBC load will be sequential (e.g. 1 partition, 1 worker) which is much less efficient where multiple workers are available.

The options describe how to partition the table when reading in parallel from multiple workers:
<ul>
<li>partitionColumn</li>
<li>lowerBound</li>
<li>upperBound</li>
<li>numPartitions.	</li>
</ul>

These options must all be specified if any of them is specified. 
<ul>
<li>partitionColumn must be a numeric column from the table in question used to partition the table.</li>

<li>Notice that lowerBound and upperBound are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned.</li>
<li>fetchSize is the JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows).</li>
</ul>
<br>
An example load command using those options might look like this (<b>Note:</b> if you partition by column then the partitioning column <b>must</b> be a number columne in the source table):

<pre lang="scala">
scala> val departments = sqlContext.read.format("jdbc")
                  .option("url", "jdbc:oracle:thin:br/hr@localhost:1521/orcl")
                  .option("driver", "oracle.jdbc.OracleDriver")
                  .option("dbtable", "departments")
                  .option("partitionColumn", "DEPARTMENT_ID")
                  .option("lowerBound", "1")
                  .option("upperBound", "10000")
                  .option("numPartitions", "4")
                  .option("fetchsize", "1000")
                  .load()
</pre>

> Don’t create too many partitions in parallel on a large cluster, otherwise Spark might crash the external database.

In our case we can't partition because the ID collumn is a text column but I can set the fetchsize:
<pre lang="scala">
val crimes = sqlContext.read.format("jdbc")
                  .option("url", "jdbc:oracle:thin:bulk_load/bulk_load@localhost:1521/orcl")
                  .option("driver", "oracle.jdbc.OracleDriver")
                  .option("dbtable", "crimes")
                  .option("fetchsize", "1000")
                  .load()
</pre>

You should experiment with these options when you load your data on your infrastructure.
You can read more about this here: http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases

At this point the JDBC statement has been validated but Spark hasn't yet checked the physical data (e.g. if you provide an invalid partitioning column you won't get an error message until you try to read the data).

Now that we've created a dataframe using the jdbc method shown above, we can use the dataframe method printSchema() to look at the dataframe schema. 
You'll notice that it looks a lot like a table. That's great because it means that we can use it to manipulate large volumes of tabular data:

<pre lang="scala">
scala> crimes.printSchema()
root
 |-- ID: string (nullable = false)
 |-- CASE_NUMBER: string (nullable = true)
 |-- INCIDENT_DATE: string (nullable = true)
 |-- BLOCK: string (nullable = true)
 |-- IUCR: string (nullable = true)
 |-- PRIMARY_TYPE: string (nullable = true)
 |-- DESCRIPTION: string (nullable = true)
 |-- LOCATION_DESCRIPTION: string (nullable = true)
 |-- ARREST: string (nullable = true)
 |-- DOMESTIC: string (nullable = true)
 |-- BEAT: string (nullable = true)
 |-- DISTRICT: string (nullable = true)
 |-- WARD: string (nullable = true)
 |-- COMMUNITY_AREA: string (nullable = true)
 |-- FBI_CODE: string (nullable = true)
 |-- X_COORDINATE: string (nullable = true)
 |-- Y_COORDINATE: string (nullable = true)
 |-- YEAR: string (nullable = true)
 |-- UPDATED_ON: string (nullable = true)
 |-- LATITUDE: string (nullable = true)
 |-- LONGITUDE: string (nullable = true)
 |-- LOCATION: string (nullable = true)
</pre>

We can use the dataframe .show() method to display the first x:int rows in the table:
<pre lang="scala">
scala> crimes.show(5)
+-------+-----------+--------------------+--------------------+----+-------------------+--------------------+--------------------+------+--------+----+--------+----+--------------+--------+------------+------------+----+--------------------+------------+-------------+--------------+
|     ID|CASE_NUMBER|       INCIDENT_DATE|               BLOCK|IUCR|       PRIMARY_TYPE|         DESCRIPTION|LOCATION_DESCRIPTION|ARREST|DOMESTIC|BEAT|DISTRICT|WARD|COMMUNITY_AREA|FBI_CODE|X_COORDINATE|Y_COORDINATE|YEAR|          UPDATED_ON|    LATITUDE|    LONGITUDE|      LOCATION|
+-------+-----------+--------------------+--------------------+----+-------------------+--------------------+--------------------+------+--------+----+--------+----+--------------+--------+------------+------------+----+--------------------+------------+-------------+--------------+
|     ID|Case Number|                Date|               Block|IUCR|       Primary Type|         Description|Location Description|Arrest|Domestic|Beat|District|Ward|Community Area|FBI Code|X Coordinate|Y Coordinate|Year|          Updated On|    Latitude|    Longitude|      Location|
|3666345|   HK764692|11/18/2004 12:00:...|    118XX S LOWE AVE|0890|              THEFT|       FROM BUILDING|           RESIDENCE| false|   false|0524|     005|  34|            53|      06|     1174110|     1826325|2004|04/15/2016 08:55:...| 41.67883435|-87.638323571| "(41.67883435|
|3666346|   HK757211|11/17/2004 08:00:...|014XX N MASSASOIT...|0560|            ASSAULT|              SIMPLE|              STREET| false|   false|2531|     025|  29|            25|     08A|     1137744|     1909068|2004|04/15/2016 08:55:...|41.906623511|-87.769453017|"(41.906623511|
|3666347|   HK764653|11/21/2004 10:00:...|  130XX S DREXEL AVE|0910|MOTOR VEHICLE THEFT|          AUTOMOBILE|CHA PARKING LOT/G...| false|   false|0533|     005|   9|            54|      07|     1184304|     1818785|2004|04/15/2016 08:55:...|41.657911807|-87.601244288|"(41.657911807|
|3666348|   HK761590|11/19/2004 07:45:...|063XX S HAMILTON AVE|0430|            BATTERY|AGGRAVATED: OTHER...|            SIDEWALK| false|   false|0726|     007|  15|            67|     04B|     1163120|     1862567|2004|04/15/2016 08:55:...|41.778524374|-87.677540732|"(41.778524374|
+-------+-----------+--------------------+--------------------+----+-------------------+--------------------+--------------------+------+--------+----+--------+----+--------------+--------+------------+------------+----+--------------------+------------+-------------+--------------+
only showing top 5 rows
</pre>

<h3>Change DataFrame Column Names To Lower Case</h3>
Go back to the window running the Spark REPL.

Before we can move the data to Cassandra we have to work around a 'feature' of the spark-cassandra connector. If you try to save your dataframe to Cassandra now it will fail with an error message saying that none of the columns exist e.g. 
<pre lang="sql">
java.util.NoSuchElementException: Columns not found in table bulk_load.crimes: ID, CASE_NUMBER, INCIDENT_DATE
</pre>
It does this even though they do exist in both tables. The problem is that the connector expects the column title text case in the dataframe to match the column title text case in the Cassandra table. In Cassandra, column names/titles are always in lower case, so the dataframe names must match.

So we need to modify our dataframe schema.... 

DataFrames are immutable in Spark, so we can't modify the one we have - we need to create a new dataframe with the correct column titles.

<h3>Create A New Lower-Case Column Name List</h3>
<pre lang="scala">
scala> val newNames = Seq("id", "case_number", "incident_date", "block","iucr","primary_type","description","location_description","arrest","domestic","beat","district","ward","community_area","fbi_code","x_coordinate","y_coordinate","year","updated_on","latitude","longitude", "location"); 
</pre>

Response:
<pre lang="scala">
newNames: Seq[String] = List(id, case_number, incident_date, block, iucr, primary_type, description, location_description, arrest, domestic, beat, district, ward, community_area, fbi_code, x_coordinate, y_coordinate, year, updated_on, latitude, longitude, location)
</pre>

<h3>Create A New DataFrame With Lower-Case Column Names</h3>
Now create a new dataframe based on the old one with lower case column titles:
<pre lang="scala">
scala> val crimes_lc = crimes.toDF(newNames: _*)
</pre>

Response:
<pre lang="scala">
crimes_lc: org.apache.spark.sql.DataFrame = [id: string, case_number: string, incident_date: string, block: string, iucr: string, primary_type: string, description: string, location_description: string, arrest: string, domestic: string, beat: string, district: string, ward: string, community_area: string, fbi_code: string, x_coordinate: string, y_coordinate: string, year: string, updated_on: string, latitude: string, longitude: string, location: string]
</pre>
And now if we look at the schema of our new dataframe - hey presto! - it's got lower case column titles.
<pre lang="scala">
scala> crimes_lc.printSchema()
root
 |-- id: string (nullable = false)
 |-- case_number: string (nullable = true)
 |-- incident_date: string (nullable = true)
 |-- block: string (nullable = true)
 |-- iucr: string (nullable = true)
 |-- primary_type: string (nullable = true)
 |-- description: string (nullable = true)
 |-- location_description: string (nullable = true)
 |-- arrest: string (nullable = true)
 |-- domestic: string (nullable = true)
 |-- beat: string (nullable = true)
 |-- district: string (nullable = true)
 |-- ward: string (nullable = true)
 |-- community_area: string (nullable = true)
 |-- fbi_code: string (nullable = true)
 |-- x_coordinate: string (nullable = true)
 |-- y_coordinate: string (nullable = true)
 |-- year: string (nullable = true)
 |-- updated_on: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- location: string (nullable = true)
</pre>
Now we can proceed to write this data to Cassandra.

<h2>Write The Crimes Data From Oracle To Cassandra</h2>
Save our new dataframe CRIMES_LC to Cassandra;
<pre lang="scala">
scala> crimes_lc.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "crimes", "keyspace" -> "bulk_load")).save()

scala> 
</pre>

This will take a while to complete - 32 minutes in my case. While it's in progress you may be able to check and see records appearing in the table although when it was in full swing I did get time-out errors when trying to query the table in cqlsh :[

On my single-node VM running Oracle 12c, DSE/Cassandra and DSE/Spark I was averaging about 3000 Oracle->Spark->Cassandra rows written per second, so the import of 6.1m rows finished close to the anticipated time of 33 minutes.
YMMV

<pre lang="sql">
cqlsh:bulk_load> select count(*) from crimes;

 count
--------
 120428

(1 rows)
</pre>

You can track progress from your Spark Master UI - this should be at 
http://[your-host-name]:7080/

At the end of the run you can query the data in the Cassandra CRIMES tables by partition key:
<pre>
cqlsh:bulk_load> select * from crimes where id='3666345';

 id      | arrest | beat | block            | case_number | community_area | description   | district | domestic | fbi_code | incident_date          | iucr | latitude    | location      | location_description | longitude     | primary_type | updated_on             | ward | x_coordinate | y_coordinate | year
---------+--------+------+------------------+-------------+----------------+---------------+----------+----------+----------+------------------------+------+-------------+---------------+----------------------+---------------+--------------+------------------------+------+--------------+--------------+------
 3666345 |  false | 0524 | 118XX S LOWE AVE |    HK764692 |             53 | FROM BUILDING |      005 |    false |       06 | 11/18/2004 12:00:00 PM | 0890 | 41.67883435 | "(41.67883435 |            RESIDENCE | -87.638323571 |        THEFT | 04/15/2016 08:55:02 AM |   34 |      1174110 |      1826325 | 2004

</pre>

At the end of the run I used nodetool to tell me how much space had been occupied in Cassandra:
<pre>
$ nodetool tablestats bulk_load
Keyspace: bulk_load
	Read Count: 0
	Read Latency: NaN ms.
	Write Count: 6156888
	Write Latency: 0.027655199509882267 ms.
	Pending Flushes: 0
		Table: crimes
		SSTable count: 6
		Space used (live): 1704215924
		Space used (total): 1704215924
		Space used by snapshots (total): 0
		Off heap memory used (total): 8543995
		SSTable Compression Ratio: 0.0
		Number of keys (estimate): 6111179
		Memtable cell count: 14452
		Memtable data size: 7936758
		Memtable off heap memory used: 0
</PRE>

<h2>ASCII Storage - Analysis</h2>
We now have the sizes of the data in each of the three formats:
<ul>
<li>CSV (text) 1.4 GB<l/i>
<li>Oracle (as text) 1.7 GB</li>
<li>Cassandra (as text) 1.7 GB - uncompressed</li>
<li>Cassandra (as text) 876 MB - compressed</li>
</ul>

In the case of compressed data, stored with a real-world Cassandra data replication factor of 3, the volume of data in a multi-node Cassandra datacenter would be 876 MB x 3 = 2,700 MB.
Assuming the customer has fairly fast machines with fast, locally attached SSD storage, each DSE/Cassandra node might be expected to store 1 TB, so an initial cluster size of 3-5 machines would be recommended.

In a use case with 1.7 GB of uncompressed data the total volume when replicated 3 times would be 5.1 GB, suggesting a cluster size of minimum 5 nodes.

<h2>Migration & Data Modelling</h2>
Remember that this is just one test, with just one table. in a "real-world" situation there may potentially be only a few tables, or a large number of tables. This data will almost certainly need to be re-modelled, not just copied, when it moves from an Oracle relational model to a distributed, replicated NoSQL database model. The re-modelling, or transformation, usually involves an element of data de-normalisation and duplication in order to optimise query efficiency, and in some cases this may have a significant impact on the volume of storage and the consequent number of DSE/Cassandra nodes that are required to accommodate the same volume of data in DSE/Cassandra.<p>
<br>



<H1>Appendix 1 - Other Stuff</H1>
<i>An alternative interesting-looking source file is an airline flight performance statistics 
http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time
the file expands to 200MB

<pre>
-rw-rw-r--   1 oracle oracle  22646800 Sep 14 09:58 On_Time_On_Time_Performance_2016_1.zip
-rw-r--r--   1 oracle oracle 200290882 Mar 14  2016 On_Time_On_Time_Performance_2016_1.csv
</pre>
</i>



