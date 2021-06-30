import cassandra

from cassandra.cluster import Cluster
try:
    cluster=Cluster(['127.0.0.1'])  #if we have locka install
    session=cluster.connect()
except Exception as e:
    print(e)



try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS george 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

try:
    session.set_keyspace('george')
except Exception as e:
    print(e)


try:
    session.set_keyspace('george')
except Exception as e:
    print(e)


#
#
#
#Create the tables

query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "CREATE TABLE IF NOT EXISTS artist_library "
query = query + "(artist_name text, year int, album_name text, PRIMARY KEY (artist_name,  year))"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "CREATE TABLE IF NOT EXISTS album_library "
query = query + "(artist_name text, album_name text, year int, PRIMARY KEY (album_name, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)



#
#
#insert data into tables


query = "INSERT INTO music_library (year, artist_name, album_name)"
query = query + " VALUES (%s, %s, %s)"

query1 = "INSERT INTO artist_library (artist_name, year, album_name)"
query1 = query1 + " VALUES (%s, %s, %s)"

query2 = "INSERT INTO album_library (album_name, artist_name, year)"
query2 = query2 + " VALUES (%s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1965, "The Who", "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1966, "The Monkees", "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1970, "The Carpenters", "Close To You"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Beatles", 1970, "Let it Be"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Beatles", 1965, "Rubber Soul"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Who", 1965, "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Monkees", 1966, "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Carpenters", 1970, "Close To You"))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("Let it Be", "The Beatles", 1970))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("Rubber Soul", "The Beatles", 1965))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("My Generation", "The Who", 1965))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("The Monkees", "The Monkees", 1966))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("Close To You", "The Carpenters", 1970))
except Exception as e:
    print(e)




#
#
#
#Validateour model

query = "select * from music_library WHERE YEAR=1970"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.artist_name, row.album_name, )

query = "select * from artist_library WHERE ARTIST_NAME='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.artist_name, row.year, row.album_name)

query = "select * from album_library WHERE ALBUM_NAME='Close To You'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.artist_name, row.year, row.album_name)



#
#
#
#For the sake of the demo, drop the table.


query = "drop table music_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table album_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table artist_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
#
#
#
#### Close the session and cluster connection