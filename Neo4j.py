from neo4j.v1 import GraphDatabase, basic_auth
import datetime
import time

# Drop all data
def drop (session):
  session.run ("MATCH (n) DETACH DELETE n")

# Print all data
def print_all(session):
  print('The program have inserted the following nodes:')
  for item in session.run('MATCH (n) RETURN n'):
    print(item)

# Create order node with needed attributes
def create_order (session, id, orderKey, custKey, orderDate, shipPriority):
  session.run ("CREATE (" + id + ":Order {orderkey:'" + orderKey +
               "', custkey:'" + custKey + "', orderdate: '" + orderDate + "', shippriority: '" + shipPriority + "'})")

# Create lineitem node with needed attributes
def create_lineitem (session, id, orderKey, suppKey, returnFlag, lineStatus, quantity, extendedPrice, discount, tax, shipDate):
  session.run ("CREATE (" + id + ":LineItem {orderkey:'" + orderKey +
               "', suppkey:'" + suppKey + "', returnflag: '" + returnFlag + "', linestatus: '" + lineStatus +
               "', quantity: '" + quantity + "', extendedprice: '" + extendedPrice + "', discount: '" + discount +
               "', tax: '" + tax + "', shipdate: '" + shipDate + "'})")

# Create customer node with needed attributes
def create_customer (session, id, custKey, nationKey, mktSegment):
  session.run ("CREATE (" + id + ":Customer {custkey:'" + custKey +
               "', nationkey:'" + nationKey + "', mktsegment: '" + mktSegment + "'})")

# Create nation node with needed attributes
def create_nation (session, id, nationKey, regionKey, name):
  session.run ("CREATE (" + id + ":Nation {nationkey:'" + nationKey +
               "', regionkey:'" + regionKey + "', name: '" + name + "'})")

# Create region node with needed attributes
def create_region (session, id, regionKey, name):
  session.run ("CREATE (" + id + ":Region {regionkey:'" + regionKey +
               "', name: '" + name + "'})")

# Create supplier node with needed attributes
def create_supplier (session, id, suppKey, nationKey, name, acctbal, address, phone):
  session.run ("CREATE (" + id + ":Supplier {suppkey: '" + suppKey + "', nationkey: '" + nationKey +
              "', name: '" + name + "', acctbal: '" + acctbal + "', address: '" + address + "', phone: '" + phone + "'})")

# Create partSupp node with needed attributes
def create_partsupp (session, id, suppKey, partKey, supplyCost):
  session.run ("CREATE (" + id + ":PartSupp {partkey: '" + partKey + "', suppKey: '" + suppKey +
              "', supplycost: '" + supplyCost + "'})")

# Create part node with needed attributes
def create_part (session, id, partKey, mfgr, type):
  session.run ("CREATE (" + id + ":Part {partkey: '" + partKey + "', mfgr: '" + mfgr +
              "', type: '" + type + "'})")

# Create an edge between order and lineItem nodes
def create_edge_order_lineitem (session, order, orderkey, lineitem):
  session.run ("MATCH (" + order + ":Order {orderkey: '" + orderkey + "'}), (" + lineitem +
               ":LineItem {orderkey: '" + orderkey + "'}) CREATE (" + order + ")-[:has]->(" + lineitem + ")")

# Create an edge between order and customer nodes
def create_edge_customer_order (session, customer, custKey, order):
  session.run ("MATCH (" + customer + ":Customer {custkey: '" + custKey + "'}), (" + order +
               ":Order {custkey: '" + custKey + "'}) CREATE (" + customer + ")-[:buy]->(" + order + ")")

# Create an edge between customer and nation nodes
def create_edge_nation_customer (session, nation, nationKey, customer):
  session.run ("MATCH (" + nation + ":Nation {nationkey: '" + nationKey + "'}), (" + customer +
               ":Customer {nationkey: '" + nationKey + "'}) CREATE (" + nation + ")-[:belongs]->(" + customer + ")")

# Create an edge between nation and region nodes
def create_edge_region_nation (session, region, regionKey, nation):
  session.run ("MATCH (" + region +":Region {regionkey: '" + regionKey + "'}), (" + nation +
               ":Nation {regionkey: '" + regionKey + "'}) CREATE (" + region + ")-[:belongs]->(" + nation + ")")

# Create an edge between nation and supplier nodes
def create_edge_nation_supplier (session, nation, nationKey, supplier):
  session.run ("MATCH (" + nation + ":Nation {nationkey: '" + nationKey + "'}), (" + supplier +
               ":Supplier {nationkey: '" + nationKey + "'}) CREATE (" + nation + ")-[:belongs]->(" + supplier + ")")

# Create an edge between lineitem and supplier nodes
def create_edge_supplier_lineitem (session, supplier, suppKey, lineItem):
  session.run ("MATCH (" + supplier + ":Supplier {suppkey: '" + suppKey + "'}), (" + lineItem +
               ":LineItem {suppkey: '" + suppKey + "'}) CREATE (" + supplier + ")-[:has]->(" + lineItem + ")")

# Create an edge between supplier and partSupp nodes
def create_edge_supplier_partsupp (session, supplier, suppKey, partSupp):
  session.run ("MATCH (" + supplier + ":Supplier {suppkey: '" + suppKey + "'}), (" + partSupp +
               ":PartSupp {suppkey: '" + suppKey + "'}) CREATE (" + supplier + ")-[:hola]->(" + partSupp + ")")

# Create an edge between partSupp and part nodes
def create_edge_partsupp_part (session, partSupp, partKey, part):
  session.run ("MATCH (" + partSupp + ":PartSupp {partkey: '" + partKey + "'}), (" + part +
               ":Part {partkey: '" + partKey + "'}) CREATE (" + partSupp + ")-[:has]->(" + part + ")")

# Insert some data in database
def insertDatabase (db):
    print ('Starting inserts...')
    session = db.session()

    orderDate = datetime.datetime (2018, 11, 23)
    shipDate = datetime.datetime (2018, 11, 25)
    orderTime = str (time.mktime (orderDate.timetuple()))
    shipTime = str (time.mktime (shipDate.timetuple()))

    drop (session)
    # Insert all create statements
    create_order (session, 'o1', 'o1', 'c1', orderTime, '1')
    create_lineitem (session, 'l1', 'o1', 's11', 'a', 'a', '10', '10.0', '0.1', '2.0', shipTime)
    create_customer (session, 'c1', 'c1', 'n1', 'MKT1')
    create_nation (session, 'n1', 'n1', 'r1', 'SPAIN')
    create_region (session, 'r1', 'r1', 'EUROPE')
    create_supplier (session, 's1', 's11', 'n1', 'SUPPLY', '20', 'Pedrell, 2', '627430662')
    create_partsupp (session, 'ps1', 's11', 'p1', '23.0')
    create_part (session, 'p1', 'p1', 'mfgr', 'type1')

    create_edge_order_lineitem (session, 'o1', 'o1', 'l1')
    create_edge_supplier_lineitem (session, 's1', 's11', 'l1')
    create_edge_supplier_partsupp (session, 's1', 's11', 'ps1')
    create_edge_partsupp_part (session, 'ps1', 'p1', 'p1')
    create_edge_customer_order (session, 'c1', 'c1','o1')
    create_edge_nation_customer (session, 'n1', 'n1', 'c1')
    create_edge_nation_supplier (session, 'n1', 'n1', 's11')
    create_edge_region_nation (session, 'r1', 'r1', 'n1')

    print_all (session)
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
  #executeQueries (db)


if __name__ == '__main__':
  main()