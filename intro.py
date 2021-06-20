import cassandra

from cassandra.cluster import Cluster
try:
    cluster=Cluster(['127.0.0.1'])  #if we have locka install
    session=cluster.connect()
except Exception as e:
    print(e)