<h1>Migrating A Large Data Set From CSV To Oracle To Cassandra - A look at storage impact</h1>
The objective of this exercise is to demonstrate how the migration of data from CSV to Oracle to Cassandra changes the required storage volume of the data. 

I'll be using the DataFrame capability introduced in Apache Spark 1.3 to load data from tables in an Oracle database (12c) via Oracle's JDBC thin driver, then save from Oracle to Cassandra.

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

<br>
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
Type exist in cqlsh to return to the shell prompt.
<br>

<H2>Identify Spark Master</h2>
We use the new DSE 5.0 format for the dse tool to get the address of the Spark Master in our cluster. 
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

Wordcount tells me there are just over 6 million records (about 1.5 GB CSV file):
<pre>
$ cat Crimes_-_2001_to_present.csv | wc
 6156888 69665305 1449194734
</pre>
<br>
<h2>Create An Oracle User</h2>
We need an owner of the tables we're going to create.
<pre lang="sql">
SQL> connect sys as sysdba
Enter password: 
Connected.
</pre>
Run this command to allow local User ID's to be created:

<pre lang="sql">
SQL> alter session set "_ORACLE_SCRIPT"=true; 
</pre>

<pre lang="sql">
SQL> create user bulk_load identified by bulk_load;

User created.
</pre>

<pre lang="sql">
SQL> grant connect, resource, dba to bulk_load;

Grant succeeded.
</pre>

<pre lang="sql">
SQL> alter user bulk_load default tablespace "USERS";

User altered.
</pre>

<pre lang="sql">
SQL> alter user bulk_load temporary tablespace "TEMP";

User altered.
</pre>
<br>
<h2>Create An External File Directory</h2>
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


<h2>Create An External File Directory</h2>
This is how Oracle reads a text file and manages it as an external table.
Connect as our user:
<pre lang="sql">
SQL> connect bulk_load/bulk_load;
Connected.
</pre>
Create the 'virtual' table based on the source data file.
>The data is rather inconsistent in terms of enforcing format and type. I don't want to have to spend a lifetime data-cleansing all 6.1 m records before starting this project so tyhr initial load into Oracle is in text format.
In the first test we will see what happens to data when it is transferred into Oracle and then to DSE/Cassandra, all in text format. This will make an interesting baseline comparison.
The second part of the exercise will be to build a more representative test by storing the data using a wider variety of datatypes, e.g. decimal, binary, date etc.
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

If we describe the 'table' it matches the format of the file it's based on:
<pre lang="sql">
SQL> desc xternal_crime_data
 Name					   Null?    Type
 ----------------------------------------- -------- ----------------------------
 Name					   Null?    Type
 ----------------------------------------- -------- ----------------------------
 ID						    VARCHAR2(30)
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

Now tell me how many records there are in my external 'table' - just over 6 million:
<pre lang="sql">
SQL> select count(*) from xternal_crime_data;

  COUNT(*)
----------
   6156888
</pre>

You can watch for errors or bad records using tail or less on these files - log output will be to the default directory that was specified in the create external table statement:
<pre>
$ rm /app/oracle/downloads/*.bad
$ rm /app/oracle/downloads/*.log
</pre>

Nothing happens until you attempt to read data from the table. You can track progress by using tail -f or less to view the log and bad files.

<h2>Create A Table, Physically Load Data Into Oracle</h2>
Create a physical table in the Oracle database using the "...as select" SQL syntax:
<pre lang="sql">
SQL> create table crime_data as select * from xternal_crime_data;
</pre>

Let's put a primary key and index on that table:
<pre lang="sql">
SQL> create index crime_id on crime_data(id);

Index created.
</pre>

<pre lang="sql">
SQL> alter table crime_data add constraint crime_data_pk primary key(id);

Table altered.
</pre>

We now have an index on our primary key which is now showing as a "NOT NULL" column.
Of course in the real-world, we would make Crime ID a unique index, but for this exercise I wanted to avoid having to worry about duplicate keys.

<pre lang="sql">
SQL> desc crime_data
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
<pre lang="sql">
SQL> connect / as sysdba
</pre>

<pre lang="sql">
SQL> select table_name from dba_tables where owner = 'BULK_LOAD';

TABLE_NAME
--------------------------------------------------------------------------------
CRIME_DATA
XTERNAL_CRIME_DATA
</pre>

Run this query to determine total storage for a user including indexes, blobs etc. You will be prompted for the Oracle user to check, in uppercase - in my case BULK_LOAD:

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
BULK_LOAD  CRIME_DATA				  1672	      100
</pre>


So a 1.4GB CSV file has become 1.7GB of storage in Oracle.<p>
<br>
<H1>Copy Data From Oracle To Cassandra</H1>
We now have 1.6GB of data in Oracle. 
The next objective is to see how much space that 1.7GB occupies when it is copied to Cassandra, and what impact Cassandra on-disk compression and the new Cassandra 3.0 storage engine will have on those numbers.


So all is looking good on the Oracle side. Now time to turn to Cassandra and Spark.

<h2>Download Oracle ojdbc7.jar</h2>
We have to add the Oracle JDBC jar file to our Spark classpath so that Spark knows how to talk to the Oracle database.


<br>
<h2>Using The Oracle JDBC Driver</h2>
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

<h3>Restart DSE</h3>
<pre>
$ sudo service dse stop
$ sudo service dse start
</pre>


<h2>Start The Spark REPL</h2>
I'm passing to the path to the ojdbc7.jar file on the command line (shouldn't be needed as the driver path is defined in the spark-defaults.conf file now, but it seems not to work without it).

<pre>
$ dse spark --driver-class-path /app/oracle/downloads/ojdbc7.jar -deprecation
</pre>


<pre lang="scala">
scala> val crimes = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:bulk_load/bulk_load@localhost:1521/orcl", "dbtable" -> "crime_data"))
</pre>
<pre lang="scala">
67: warning: method load in class SQLContext is deprecated: Use read.format(source).options(options).load(). This will be removed in Spark 2.0.
         val crimes = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:bulk_load/bulk_load@localhost:1521/orcl", "dbtable" -> "crime_data"))
</pre>
There's a deprecation warning message that you can ignore.
Here's the output we're looking for, confirming that we established a valid connection to the remote database.
<pre lang="scala">
crimes: org.apache.spark.sql.DataFrame = [ID: string, CASE_NUMBER: string, INCIDENT_DATE: string, BLOCK: string, IUCR: string, PRIMARY_TYPE: string, DESCRIPTION: string, LOCATION_DESCRIPTION: string, ARREST: string, DOMESTIC: string, BEAT: string, DISTRICT: string, WARD: string, COMMUNITY_AREA: string, FBI_CODE: string, X_COORDINATE: string, Y_COORDINATE: string, YEAR: string, UPDATED_ON: string, LATITUDE: string, LONGITUDE: string, LOCATION: string]
</pre>
Now that we've created a dataframe using the jdbc method shown above, we can use printSchema() to look at the dataframe schema. You'll notice that it looks a lot like a table. That's deliberate because it means that we can use it to manipulate large volumes of tabular data:

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

We can use the dataframe .show() method to display the first 20 rows in the table:
<pre lang="scala">
scala> crimes.show()
+--------+-----------+--------------------+--------------------+----+-------------------+--------------------+--------------------+------+---------+-----+--------+----+--------------+--------+------------+------------+-------+--------------------+------------+--------------------+--------------+
|      ID|CASE_NUMBER|       INCIDENT_DATE|               BLOCK|IUCR|       PRIMARY_TYPE|         DESCRIPTION|LOCATION_DESCRIPTION|ARREST| DOMESTIC| BEAT|DISTRICT|WARD|COMMUNITY_AREA|FBI_CODE|X_COORDINATE|Y_COORDINATE|   YEAR|          UPDATED_ON|    LATITUDE|           LONGITUDE|      LOCATION|
+--------+-----------+--------------------+--------------------+----+-------------------+--------------------+--------------------+------+---------+-----+--------+----+--------------+--------+------------+------------+-------+--------------------+------------+--------------------+--------------+
|      ID|Case Number|                Date|               Block|IUCR|       Primary Type|         Description|Location Description|Arrest| Domestic| Beat|District|Ward|Community Area|FBI Code|X Coordinate|Y Coordinate|   Year|          Updated On|    Latitude|           Longitude|      Location|
| 3666345|   HK764692|11/18/2004 12:00:...|    118XX S LOWE AVE|0890|              THEFT|       FROM BUILDING|           RESIDENCE| false|    false| 0524|     005|  34|            53|      06|     1174110|     1826325|   2004|04/15/2016 08:55:...| 41.67883435|       -87.638323571| "(41.67883435|
| 3666346|   HK757211|11/17/2004 08:00:...|014XX N MASSASOIT...|0560|            ASSAULT|              SIMPLE|              STREET| false|    false| 2531|     025|  29|            25|     08A|     1137744|     1909068|   2004|04/15/2016 08:55:...|41.906623511|       -87.769453017|"(41.906623511|
| 3666347|   HK764653|11/21/2004 10:00:...|  130XX S DREXEL AVE|0910|MOTOR VEHICLE THEFT|          AUTOMOBILE|CHA PARKING LOT/G...| false|    false| 0533|     005|   9|            54|      07|     1184304|     1818785|   2004|04/15/2016 08:55:...|41.657911807|       -87.601244288|"(41.657911807|
| 3666348|   HK761590|11/19/2004 07:45:...|063XX S HAMILTON AVE|0430|            BATTERY|AGGRAVATED: OTHER...|            SIDEWALK| false|    false| 0726|     007|  15|            67|     04B|     1163120|     1862567|   2004|04/15/2016 08:55:...|41.778524374|       -87.677540732|"(41.778524374|
| 3666350|   HK765337|11/21/2004 06:19:...| 061XX N LINCOLN AVE|1330|  CRIMINAL TRESPASS|             TO LAND|  SMALL RETAIL STORE|  true|    false| 1711|     017|  50|            13|      26|     1152804|     1941021|   2004|04/15/2016 08:55:...|41.994019837|       -87.713281946|"(41.994019837|
|10504143|   HZ245011|04/29/2016 01:34:...|    002XX W 104TH ST|2820|      OTHER OFFENSE|    TELEPHONE THREAT|           RESIDENCE| false|    false| 0512|     005|  34|            49|      26|     1176442|     1835975|   2016|05/06/2016 03:50:...|41.705263447|       -87.629498947|"(41.705263447|
| 3666351|   HK766200|11/21/2004 09:00:...|  014XX N STATE PKWY|0820|              THEFT|      $500 AND UNDER|              STREET| false|    false| 1824|     018|  43|             8|      06|     1176025|     1910070|   2004|04/15/2016 08:55:...| 41.90859641|       -87.628801757| "(41.90859641|
| 3666352|   HK756872|11/17/2004 05:00:...|   045XX W WILCOX ST|1320|    CRIMINAL DAMAGE|          TO VEHICLE|              STREET| false|    false| 1113|     011|  28|            26|      14|     1146197|     1898954|   2004|04/15/2016 08:55:...|41.878712886|       -87.738658886|"(41.878712886|
| 3666354|   HK765887|11/19/2004 10:30:...|  035XX S PAULINA ST|0610|           BURGLARY|      FORCIBLE ENTRY|             "SCHOOL|PUBLIC|BUILDING"|false|   false|0922|           009|      11|          59|          05|1165596|             1881229|        2004|04/15/2016 08:55:...|  41.829682911|
| 3666356|   HK766293|11/21/2004 06:00:...|     006XX W 74TH ST|1310|    CRIMINAL DAMAGE|         TO PROPERTY|           APARTMENT| false|    false| 0732|     007|  17|            68|      14|     1173139|     1855820|   2004|04/15/2016 08:55:...|41.759794269|       -87.641009607|"(41.759794269|
| 3666357|   HK766287|11/21/2004 05:00:...|  083XX S COLFAX AVE|1320|    CRIMINAL DAMAGE|          TO VEHICLE|           RESIDENCE| false|    false| 0423|     004|   7|            46|      14|     1194953|     1850154|   2004|04/15/2016 08:55:...|41.743736757|       -87.561249019|"(41.743736757|
| 3666358|   HK763951|11/20/2004 11:05:...|082XX S DR MARTIN...|0860|              THEFT|        RETAIL THEFT|         GAS STATION| false|    false| 0631|     006|   6|            44|      06|     1180338|     1850357|   2004|04/15/2016 08:55:...|41.744641174|       -87.614792711|"(41.744641174|
| 3666359|   HK764785|11/20/2004 09:00:...|   056XX N MOZART ST|1320|    CRIMINAL DAMAGE|          TO VEHICLE|              STREET| false|    false| 2011|     020|  40|             2|      14|     1156332|     1937421|   2004|04/15/2016 08:55:...|41.984070441|       -87.700402289|"(41.984070441|
| 3666360|   HK755931|11/17/2004 10:00:...|  030XX N MOBILE AVE|0484|            BATTERY|PRO EMP HANDS NO/...|             "SCHOOL|PUBLIC|BUILDING"| true|   false|2511|           025|      36|          19|         08B|1133881|             1919540|        2004|04/15/2016 08:55:...|  41.935428738|
| 3666362|   HK765213|11/21/2004 04:00:...|  046XX N ELSTON AVE|0486|            BATTERY|DOMESTIC BATTERY ...|           APARTMENT|  true|     true| 1722|     017|  39|            14|     08B|     1146545|     1930640|   2004|04/15/2016 08:55:...|41.965655711|        -87.73657143|"(41.965655711|
| 3666363|   HK716111|10/29/2004 02:15:...|0000X N LAVERGNE AVE|2095|          NARCOTICS|ATTEMPT POSSESSIO...|            SIDEWALK|  true|    false| 1533|     015|  28|            25|      18|     1143052|     1899700|   2004|04/15/2016 08:55:...|41.880819219|       -87.750188229|"(41.880819219|
| 3666364|   HK764786|11/21/2004 12:10:...|     005XX N RUSH ST|0810|              THEFT|           OVER $500|PARKING LOT/GARAG...| false|    false| 1834|     018|  42|             8|      06|     1177011|     1903813|   2004|04/15/2016 08:55:...|41.891404632|       -87.625369395|"(41.891404632|
| 3666365|   HK762241|11/20/2004 03:45:...|  061XX S JUSTINE ST|0486|            BATTERY|DOMESTIC BATTERY ...|           APARTMENT| false|     true| 0713|     007|  15|            67|     08B|     1167059|     1864042|   2004|04/15/2016 08:55:...| 41.78248862|       -87.663057935| "(41.78248862|
| 3666366|   HK765012|11/21/2004 02:20:...|016XX N SPAULDING...|0486|            BATTERY|DOMESTIC BATTERY ...|           APARTMENT| false|     true| 1422|     014|  26|            23|     08B|     1153891|     1910739|   2004|04/15/2016 08:55:...|41.910902295|       -87.710093524|"(41.910902295|
+--------+-----------+--------------------+--------------------+----+-------------------+--------------------+--------------------+------+---------+-----+--------+----+--------------+--------+------------+------------+-------+--------------------+------------+--------------------+--------------+
only showing top 20 rows

</pre>

<h2>Create Cassandra KeySpace</h2>
The first thing that we need to do in Cassandra is create a keyspace to contain the table that we will create. I'm using a replication factor of 1 because I have one node in my development cluster. For most production deployments we recommend a multi-datacenter Active-Active HA setup across geographical regions using NetworkTopologyStrategy with RF=3:

Log into cqlsh 
> If you didn't change the IP defaults in cassandra.yaml then just type 'cqlsh' - if you changed the IP to be the host IP then you may need to supply the hostname e.g. 'cqlsh [hostname]'.

From the cqlsh prompt, create the keyspace:
<pre>
CREATE KEYSPACE IF NOT EXISTS bulk_load WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
USE bulk_load;
</pre>
<h2>Create A Cassandra Table</h2>

The source data had some inconsistencies that made it simpler just to load all the columns as text. To make the comparison accurate we will store the data in Cassandra in text format too.

We will create a Cassandra table that matches the Oracle columns, with a partition key based on the crime ID.

In cqlsh:

<pre lang="sql">
cqlsh:bulk_load> create table crimes (
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
PRIMARY KEY (id));
</pre>

Check it's all there:
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
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
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

Back to Spark...

<h2>Change DataFrame Columns To Lower Case</h2>

If you try to save your dataframe to Cassandra now (using the Spark-Cassandra connector) it will fail with an error message saying that columns don't exist e.g. 
<pre lang="sql">
java.util.NoSuchElementException: Columns not found in table bulk_load.crimes: ID, CASE_NUMBER, INCIDENT_DATE
</pre>
It does this even though they exist in both tables. The problem is that the connector expects the column title case in the dataframe to match the column title case in the Cassandra table. In Cassandra column names/titles are always in lower case, so the dataframe names must match.

So we need to modify our dataframe schema.... Here's a reminder of our raw employees dataframe schema again:
scala> crimes.printSchema()

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

DataFrames are immutable in Spark, so we can't modify the one we have - we need to create a new one with the correct column titles.

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
And now if we look at the schema of our new datafram - hey presto! - it's got lower case column titles.
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

We can get some information about data size in Cassandra from the nodetool cfstats command:
<pre>
$ nodetool cfstats bulk_load
Keyspace: bulk_load
	Read Count: 1
	Read Latency: 0.113 ms.
	Write Count: 6156888
	Write Latency: 0.0296818494992925 ms.
	Pending Flushes: 0
		Table: crimes
		SSTable count: 6
		Space used (live): 876227198
		Space used (total): 876227198
		Space used by snapshots (total): 0
		Off heap memory used (total): 8725758
		SSTable Compression Ratio: 0.46752276291246503
		Number of keys (estimate): 6109619
		Memtable cell count: 19074
		Memtable data size: 10498992
</pre>

This indicates that the size of the same dataset stored in ASCII/text format in the three formats is as follows:
- CSV (text) 1.4 GB
- Oracle (as text) 1.7 GB
- Cassandra (as text) 876MB

In the case of this data, stored with a more realistic real-world Cassandra data replication factor of 3, the volume of data in a multi-node Cassandra datacenter would be 876 MB x 3 = 2,700 MB.
Assuming the customer has fairly fast machines with fast, locally attached SSD storage, each DSE/Cassandra node might be expected to store 1 TB, so an initial cluster size of 3-5 machines would be recommended.

This might not affect any 

Now to mix it up a bit with numeric values and see how that changes things...<p>
<br>
We can drop some of the stuff we don't need anymore.
In SQL Plus:
SQL> truncate table crime_data;

Table truncated.

SQL> drop table crime_data;

Table dropped.

SQL> 

In cq;sh:
cqlsh:bulk_load> truncate table crimes;
cqlsh:bulk_load> drop table crimes;


<h2>Create Oracle and Cassandra Tables Using Numeric columns<h2>
<br><br><br>
<P>
<h1>Notes</h1>

<pre>
						Oracle			Cassandra
						--------------- --------------
ID             			varchar2(30)	bigint
Case Number				varchar2(30)	text
Date					varchar2(30)			timeuuid
Block					varchar2(60)	text
IUCR					varchar2(30)	text
Primary Type			varchar2(60)	text
Description				varchar2(120)	text
Location Description	varchar2(60)	text
Arrest					varchar2(60)	boolean
Domestic				varchar2(60)	boolean
Beat					varchar2(60)			int
District				number			int
Ward					number			int
Community Area			number			int
FBI Code				varchar2(10)	text
X Coordinate			number			bigint
Y Coordinate			number			bigint
Year					number			int
Updated On				date			timeuuid
Latitude				number			decimal
Longitude				number			decimal
Location				varchar2(30)	text
</pre>
<i>Our source file is a 
http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time
the file expands to 200MB

<pre>
-rw-rw-r--   1 oracle oracle  22646800 Sep 14 09:58 On_Time_On_Time_Performance_2016_1.zip
-rw-r--r--   1 oracle oracle 200290882 Mar 14  2016 On_Time_On_Time_Performance_2016_1.csv
</pre>
</i>



