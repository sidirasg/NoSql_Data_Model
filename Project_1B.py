#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[ ]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[ ]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[ ]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[ ]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[ ]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()

# TO-DO: Create a Keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sidiras
    WITH REPLICATION =
    { 'class': 'SimpleStrategy', 'replication_factor' : 1}"""
                    )

except Execution as e:
    print(e)



# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('sidiras')
except Exception as e:
    print(e)


"""Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.
Create queries to ask the following three questions of the data
1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
'Helper overview for query execution
loc, col_name, type
0 artist text, 
1 firstname text, 
2 gender text, 
3 iteminsession int, 
4 lastname text, \
5 length float, 
6 level text, 
7 location text, 
8 sessionid int, 
9 song int, 
10 userid int
"""


## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
query = "CREATE TABLE IF NOT EXISTS question1"
query = query + """(
                    sessionId int,
                    iteminSession int,
                    artist text,
                    song text,
                    length float,
                    PRIMARY KEY (sessionId, iteminSession)
                    )"""
try:
    session.execute(query)
except Exception as e:
    print(e)

## TO-DO: Query 1:
## Give me the artist, song title and song's length
## in the music app history that was heard during \
## sessionId = 338, and sessionId = 4

# take care of lower_case column names

query = "CREATE TABLE IF NOT EXISTS question1"
query = query + """(
                    sessionId int,
                    iteminSession int,
                    artist text,
                    song text,
                    length float,
                    PRIMARY KEY (sessionId, iteminSession)
                    )"""
try:
    session.execute(query)
except Exception as e:
    print(e)


# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
# '0 artist - '1 firstname - '2 gender - '3 iteminsession - '4 lastname -
# '5 length - '6 level - '7 location - '8 sessionid - '9 song - '10 userid text'


# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO question1 (sessionId, iteminSession, artist, song,length)"
        query = query + "VALUES (%s,%s,%s,%s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (int(line[8]), int(line[3]), str(line[0]), str(line[9]), float(line[5])))



## TO-DO: Add in the SELECT statement to verify the data was entered into the table
query = "SELECT artist, song, length FROM sidiras.question1 WHERE sessionId = 338 AND iteminSession = 4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.artist, row.song, row.length)


#COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS¶



## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
query = "CREATE TABLE IF NOT EXISTS question2"
query = query + """(
                    userId int,
                    sessionId int,
                    itemInSession int,
                    artist text,
                    song text,
                    firstName text,
                    lastName text,
                    PRIMARY KEY ((userId), sessionId, itemInSession))
                    WITH CLUSTERING ORDER BY (sessionId ASC, iteminSession ASC);"""
try:
    session.execute(query)
except Exception as e:
    print(e)


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query1 = "INSERT INTO question2 (userId, sessionId, iteminSession, artist, song, firstName, lastName)"
        query1 = query1 + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query1, (int(line[10]),int(line[8]),int(line[3]), str(line[0]), str(line[9]), str(line[1]), str(line[4])))

query1 = "SELECT artist, song, firstName, lastName FROM question2 WHERE userId = 10 AND sessionId = 182"
try:
    rows = session.execute(query1)
except Exception as e:
    print(e)

for row in rows:
    print(row.artist, row.song, row.firstname, row.lastname)



## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
query2 = "CREATE TABLE IF NOT EXISTS question3"
query2 = query2 + """(userid int, firstName text, 
                        lastName text, song text, 
                        PRIMARY KEY ((song),userid)
                        )"""
try:
    session.execute(query2)
except Exception as e:
    print(e)




file = 'event_datafile_new.csv'




with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query2 = "INSERT INTO question3 (userid, firstName, lastName, song)"
        query2 = query2 + " VALUES (%s, %s, %s, %s)"
        session.execute(query2, (int(line[10]),str(line[1]),str(line[4]),str(line[9])))

query2 = "SELECT firstName, lastName FROM question3 WHERE song = 'All Hands Against His Own'"
try:
    rows = session.execute(query2)
except Exception as e:
    print(e)

for row in rows:
    print(row.firstname, row.lastname)



#Drop the tables before closing out the sessions¶


## TO-DO: Drop the table before closing out the sessions
query = "drop table question1"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table question2"
try:
    rows = session.execute(query1)
except Exception as e:
    print(e)

query = "drop table question3"
try:
    rows = session.execute(query2)
except Exception as e:
    print(e)

#Close the session and cluster connection¶

session.shutdown()
cluster.shutdown()
f