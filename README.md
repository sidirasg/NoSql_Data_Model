**Project: Data Modeling with Cassandra**  <br><br>
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV 
files on user activity on the app.
As a data engineer  create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify 
to create the results.<br>
**Project Overview** <br>
 on data modeling with Apache Cassandra and complete an ETL pipeline using Python.<br> We  model the  data by creating tables in Apache Cassandra to run queries.
<br>We are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to m0odel and 
insert data into Apache Cassandra tables.<br>
\ETL pipeline you'd need to process this data. 
<br>
<br>
<br>
<br>
 
<br>
<br>
 **Datasets**
For this project, you'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of 
filepaths to two files in the dataset:

`event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv`
<br>
<br>
Relational databases store data in tables that have relations with other tables using foreign keys. A relational database’s approach to data modeling is table-centric. Queries must use table joins to get data from multiple tables that have a relation between them. Apache Cassandra does not have the concept of foreign keys or relational integrity. Apache Cassandra’s data model is based around designing efficient queries; queries that don’t involve multiple tables. Relational databases normalize data to avoid duplication. Apache Cassandra in contrast de-normalizes data by duplicating data in multiple tables for a query-centric data model. If a Cassandra data model cannot fully integrate the complexity of relationships between the different entities for a particular query, client-side joins in application code may be used.

<br>
<br>

Execute at the terminal pip install -r rrequirements.txt. <br>
Then we can execute Project_1B.py at 3.8 puthon

<br>
<br>


![](Apache_cass.png?raw=true)

