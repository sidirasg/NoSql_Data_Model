import cassandra



from cassandra.cluster import Cluster
try:
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)



try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sidiras 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)



try:
    session.set_keyspace('sidiras')
except Exception as e:
    print(e)



query = "CREATE TABLE IF NOT EXISTS music_library"
query = query + "(year int, artist_name text, album_name text, city text, PRIMARY KEY (artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)



### Insert the data into the tables

query = "INSERT INTO music_library (year, artist_name, album_name, city)"
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

    ### Let's Validate our Data Model -- Did it work?? If we look for Albums from The Beatles we should expect to see 2 rows.

query = "select * from music_library WHERE artist_name='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.artist_name, row.album_name, row.city)



### We have a couple of options (City and Album Name) but that will not get us the query we need which is looking for album's
# in a particular artist. Let's make a composite key of the `ARTIST NAME` and `ALBUM NAME`. This is assuming that an album name
# is unique to the artist it was created by (not a bad bet). --But remember this is just an exercise, you will need to understand your
# dataset fully (no betting!)


query = "CREATE TABLE IF NOT EXISTS music_library1 "
query = query + "(artist_name text, album_name text, year int, city text, PRIMARY KEY (artist_name, album_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "INSERT INTO music_library1 (artist_name, album_name, year, city)"
query = query + " VALUES (%s, %s, %s, %s)"

try:
    session.execute(query, ("The Beatles", "Let it Be", 1970, "Liverpool"))
except Exception as e:
    print(e)

try:
    session.execute(query, ("The Beatles", "Rubber Soul", 1965, "Oxford"))
except Exception as e:
    print(e)

try:
    session.execute(query, ("The Who", "My Generation", 1965, "London"))
except Exception as e:
    print(e)

try:
    session.execute(query, ("The Monkees", "The Monkees", 1966, "Los Angeles"))
except Exception as e:
    print(e)

try:
    session.execute(query, ("The Carpenters", "Close To You", 1970, "San Diego"))
except Exception as e:
    print(e)


### Validate the Data Model -- Did it work? If we look for Albums from The Beatles we should expect to see 2 rows.

query = "select * from music_library1 WHERE artist_name='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.artist_name, row.album_name, row.city)

### Drop the tables

query = "drop table music_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table music_library1"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


### Close the session and cluster connection
session.shutdown()
cluster.shutdown()