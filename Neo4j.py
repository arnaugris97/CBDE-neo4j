from neo4j.v1 import GraphDatabase, basic_auth
import datetime
import time

# Drop all data
def drop (session):
    session.run ("MATCH (n) DETACH DELETE n")

# Insert some data in database
def insertDatabase (db):
    print ('Starting inserts...')
    session = db.session()

    orderDate = datetime.datetime (2018, 11, 23)
    shipDate = datetime.datetime (2018, 11, 25)
    orderTime = time.mktime (date.timetuple())
    shipTime = time.mktime (date2.timetuple())

    drop (session)
    # Insert all create statements

    print ('Finish inserts!\n')

    session.close()

# Create indexes
def createIndexes (db):
    session = db.session()
    # Create index on shipdate of lineitem
    session.run ("CREATE INDEX ON :LineItem(shipdate)")
    # Create index on orderdate of order
    session.run ("CREATE INDEX ON :Order(orderdate)")
    # Close session
    session.close()

# Query 1
def query1 (db, date):
  result = \
    db.session().run () # Here Cypher code
  return result

# Query 2
def query2 (db, region, type, size):
  subquery_result = \
    db.session().run () # Here Cypher code

  result = \
    db.session().run () # Here Cypher code
  return result

# Query 3
def query3 (db, date1, date2, segment):
  result = \
    db.session().run () # Here Cypher code
  return result

# Query 4
def query4 (db, region, date):
  result = \
    db.session().run () # Here Cypher code
  return result

def printResult (queryNumber, result):

  print ("------------------------------------------Query " + str (queryNumber) + "------------------------------------------")
  i = 0

  for row in result:
    i += 1
    print (row)

  if i == 0:
    print ("There is no results for query " + str (queryNumber))


def executeQueries (db):
  # Execute query 1 and print result
  printResult (1, query1 (db, date=datetime.datetime (2018, 11, 27)))
  # Execute query 2 and print result
  printResult (2, query2 (db, region='Paris', type='type2', size=5))
  # Execute query 3 and print result
  printResult (3, query3 (db, date1=datetime.datetime (2018, 11, 27), date2=datetime.datetime (2018, 11, 15), segment='MKT5'))
  # Execute query 4 and print result
  printResult (4, query4 (db, region="Paris", date=datetime.datetime (2018, 11, 15)))

def main ():
  # Connect to database
  db = GraphDatabase.driver ("bolt://localhost", auth=basic_auth ("neo4j", "neoArnau"))
  # Insert in database
  insertDatabase (db)
  # Create database indexes
  createIndexes (db)
  # Execute 4 queries in database
  executeQueries (db)


if __name__ == '__main__':
  main()