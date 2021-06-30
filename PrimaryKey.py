import cassandra



### Create a connection to the database

from cassandra.cluster import Cluster
try:
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)


### Create a keyspace to work in



try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS george 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

#### Connect to the Keyspace. Compare this to how we had to create a new session in PostgreSQL.


try:
    session.set_keyspace('george')
except Exception as e:
    print(e)


#There is an error because there is not primary key

query = "CREATE TABLE IF NOT EXISTS ##### "
query = query + "(##### PRIMARY KEY (#####))"
try:
    session.execute(query)
except Exception as e:
    print(e)



### Let's insert the data into the table

query = "INSERT INTO ##### (year, artist_name, album_name, city)"
query = query + " VALUES (%s, %s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be", "Liverpool"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul", "Oxford"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1965, "The Who", "My Generation", "London"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1966, "The Monkees", "The Monkees", "Los Angeles"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1970, "The Carpenters", "Close To You", "San Diego"))
except Exception as e:
    print(e)


### Validate the Data Model -- Does it give you two rows?
query = "select * from ##### WHERE #####"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.artist_name, row.album_name, row.city)


### Try again - Create a new table with a composite key this time

query = "CREATE TABLE IF NOT EXISTS ##### "
query = query + "(#####)"
try:
    session.execute(query)
except Exception as e:
    print(e)

## You can opt to change the sequence of columns to match your composite key. \
## Make sure to match the values in the INSERT statement

query = "INSERT INTO ##### (year, artist_name, album_name, city)"
query = query + " VALUES (%s, %s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be", "Liverpool"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul", "Oxford"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1965, "The Who", "My Generation", "London"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1966, "The Monkees", "The Monkees", "Los Angeles"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1970, "The Carpenters", "Close To You", "San Diego"))
except Exception as e:
    print(e)



### Validate the Data Model -- Did it work?

query = "#####"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.artist_name, row.album_name, row.city)


### Drop the tables
query = "#####"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "#####"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

### Close the session and cluster connection

session.shutdown()
cluster.shutdown()