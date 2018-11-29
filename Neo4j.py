from neo4j.v1 import GraphDatabase, basic_auth
from random import randint
import datetime
import time

# Drop all data
def drop (session):
  session.run ("MATCH (n) DETACH DELETE n")

# Create order node with needed attributes
def create_order (session, id, orderKey, custKey, orderDate, shipPriority):
  session.run ("CREATE (" + id + ":Order {orderkey:'" + orderKey +
               "', custkey:'" + custKey + "', orderdate: {date}, shippriority: '" + shipPriority + "'})", {"date": orderDate})

# Create a line item node with parameters specified
def create_lineitem(session, identifier, orderkey, suppkey, returnflag,linestatus, quantity,
                    extendedPrice, discount, tax, shipdate):
    session.run("CREATE (" + identifier + ":LineItem {orderkey: '" + orderkey +
                "', suppkey: '" + suppkey + "', returnflag: '" + returnflag + "', quantity: " + quantity +
                ", extendedPrice: " + extendedPrice + ", discount: " + discount + ", tax: " + tax +
                ", shipdate: {date2}, linestatus: '" + linestatus + "'})", {"date2": shipdate})

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
def create_supplier (session, id, suppKey, nationKey, name, acctbal, address, phone, comment):
  session.run ("CREATE ( :Supplier {suppkey: '" + suppKey + "', nationkey: '" + nationKey +
              "', name: '" + name + "', acctbal: '" + acctbal + "', address: '" + address +
              "', phone: '" + phone + "', comment: '" + comment + "'})")

# Create order node with needed attributes
def create_partsupp (session, id, suppKey, patKey, supplyCost):
  session.run ("CREATE ( :PartSupp {supplycost:'" + supplyCost +
               "', partkey:'" + patKey + "', suppkey: '" + suppKey + "'})")

# Create part node with needed attributes
def create_part (session, id, partKey, mfgr, type, size):
  session.run ("CREATE (" + id + ":Part {partkey: '" + partKey + "', mfgr: '" + mfgr +
              "', type: '" + type + "', size: '"+ size + "'})")

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

# Create an edge between lineitem and supplier nodes
def create_edge_supplier_partsupp (session, supplier, suppKey, partSupp):
  session.run ("MATCH (" + supplier + ":Supplier {suppkey: '" + suppKey + "'}), (" + partSupp +
               ":PartSupp {suppkey: '" + suppKey + "'}) CREATE (" + supplier + ")-[:has]->(" + partSupp + ")")

# Create an edge between partSupp and part nodes
def create_edge_partsupp_part (session, partSupp, partKey, part):
  session.run ("MATCH (" + partSupp + ":PartSupp {partkey: '" + partKey + "'}), (" + part +
               ":Part {partkey: '" + partKey + "'}) CREATE (" + partSupp + ")-[:has]->(" + part + ")")

# Insert some data in database
def insertDatabase (db):
    print ('Starting inserts...')
    session = db.session()

    drop (session)
    # Insert all create statement

    for i in range(0, 40):
      orderDate = datetime.datetime (2018, randint(10, 12), randint(1, 30))
      orderTime = time.mktime (orderDate.timetuple())
      create_order (session, 'o' + str(i), 'o'  + str(i), 'c'  + str(i), orderTime, str(i))
      create_customer (session, 'c' + str(i), 'c' + str(i), 'n' + str(i%16), 'MKT' + str(i%6))
      create_partsupp (session, 'ps' + str(i), 's' + str(i%8), 'p' + str(i), str(randint(20, 40)))
      create_part (session, 'p' + str(i), 'p' + str(i), 'mfgr', 'type2', '5')

    for i in range(0, 80):
      shipDate = datetime.datetime (2018, randint(10, 12), randint(1, 30))
      shipTime = time.mktime (shipDate.timetuple())
      create_lineitem (session, 'l' + str(i), 'o'  + str(i%40), 's' + str(i%8), 'a', 'a', '10', '10.0', '0.1', '2.0', shipTime)

    for i in range(0, 8):
      create_supplier (session, 's' + str(i), 's'  + str(i), 'n' + str(i%16), 'SUPPLY', '20', 'Pedrell, 2', '627430662', 'comment')
      create_region (session, 'r' + str(i), 'r' + str(i), 'EUROPE')

    for i in range(0, 16):
      create_nation (session, 'n' + str(i), 'n' + str(i), 'r' + str(i%8), 'SPAIN')


    for i in range(0, 40):
      create_edge_order_lineitem (session, 'o' + str(i), 'o' + str(i), 'l' + str(i%80))
      create_edge_customer_order (session, 'c' + str(i), 'c' + str(i), 'o' + str(i))
      create_edge_partsupp_part (session, 'ps' + str(i), 'p' + str(i), 'p' + str(i))

    for i in range(0, 8):
      create_edge_supplier_lineitem (session, 's' + str(i), 's' + str(i), 'l' + str(i%80))
      create_edge_supplier_partsupp (session, 's' + str(i), 's' + str(i), 'ps' + str(i%40))
      create_edge_region_nation (session, 'r' + str(i), 'r' + str(i), 'n' + str(i%16))

    for i in range(0, 16):
      create_edge_nation_customer (session, 'n' + str(i), 'n' + str(i), 'c' + str(i%40))
      create_edge_nation_supplier (session, 'n' + str(i), 'n' + str(i), 's' + str(i%8))

    print ('Finish inserts!\n')

    session.close()

# Create indexes
def createIndexes (db):
    session = db.session()
    # Create index on shipdate of lineitem
    session.run ("CREATE INDEX ON :LineItem(shipdate)")
    # Close session
    session.close()

# Query 1
def query1 (db, date):
  result = \
    db.session().run (
                      " MATCH " +
                      "      ( li:LineItem ) " +
                      " WHERE " +
                      "      li.shipdate <= {date} " +
                      " WITH " +
                      "      li.returnflag                                    AS l_returnflag, " +
                      "      li.linestatus                                    AS l_linestatus, " +
                      "      SUM(li.quantity)                                 AS sum_qty, " +
                      "      SUM(li.extendedPrice)                            AS sum_base_price, " +
                      "      SUM(li.extendedPrice*(1-li.discount))            AS sum_disc_price, " +
                      "      SUM(li.extendedPrice*(1-li.discount)*(1+li.tax)) AS sum_charge, " +
                      "      AVG(li.quantity)                                 AS avg_qty, " +
                      "      AVG(li.extendedPrice)                            AS avg_price, " +
                      "      AVG(li.discount)                                 AS avg_disc, " +
                      "      COUNT(*)                                         AS count_order " +
                      " RETURN " +
                      "      l_returnflag, " +
                      "      l_linestatus, " +
                      "      sum_qty, " +
                      "      sum_base_price, " +
                      "      sum_disc_price, " +
                      "      sum_charge, " +
                      "      avg_qty, " +
                      "      avg_price, " +
                      "      avg_disc, " +
                      "      count_order " +
                      " ORDER BY " +
                      "      l_returnflag       ASC, " +
                      "      l_linestatus       ASC ",
                     {"date": time.mktime(date.timetuple())})
  return result

# Query 2
def query2 (db, region, type, size):
  subquery_result = \
    db.session().run (" MATCH (" +
                      "    (r:Region)-[:belongs]->(n:Nation)-[:belongs]->(s:Supplier)-[:has]->(ps:PartSupp) )" +
                      " WHERE " +
                      "    r.name = {region} " +
                      " WITH " +
                      "    MIN(ps.supplycost) AS supplycost" +
                      " RETURN " +
                      "    supplycost ",
                     {"region": region})

  global min_cost
  for item in subquery_result:
    min_cost = item['supplycost']

  result = \
    db.session().run (" MATCH (" +
                      "     (r:Region)-[:belongs]->(n:Nation)-[:belongs]->(s:Supplier)-[:has]->(ps:PartSupp)-[:has]->(p:Part) )" +
                      " WHERE " +
                      "     p.size = {size}   AND " +
                      "     p.type = {type}   AND " +
                      "     ps.supplycost = {suppcost} " +
                      " WITH " +
                      "     s.accbal   AS s_accbal, " +
                      "     s.name     AS s_name, " +
                      "     n.name     AS n_name, " +
                      "     p.partkey  AS p_partkey, " +
                      "     p.mfgr     AS p_mfgr, " +
                      "     s.adress   AS s_adress, " +
                      "     s.phone    AS s_phone, " +
                      "     s.comment  AS s_comment " +
                      " RETURN " +
                      "     s_accbal, " +
                      "     s_name, " +
                      "     n_name, " +
                      "     p_partkey, " +
                      "     p_mfgr, " +
                      "     s_adress, " +
                      "     s_phone, " +
                      "     s_comment " +
                      " ORDER BY " +
                      "     s_accbal   DESC, " +
                      "     n_name   ASC, " +
                      "     p_partkey  ASC ",
                     {"size": size,
                      "type": type,
                      "suppcost": min_cost})
  return result

# Query 3
def query3 (db, date1, date2, segment):
  result = \
    db.session().run (" MATCH " +
                      " (c:Customer)-[:buy]->(o:Order)-[:has]->(li:LineItem)" +
                      " WHERE " +
                      "   c.mktsegment = {segment}    AND " +
                      "   o.orderdate < {orderdate}   AND " +
                      "   li.shipdate > {shipdate}        " +
                      " WITH " +
                      "   li.orderkey                             AS l_orderkey, " +
                      "   o.orderdate                             AS o_orderdate, " +
                      "   o.shippriority                          AS o_shippriority, " +
                      "   SUM(li.extendedPrice*(1-li.discount))   AS revenue " +
                      " RETURN " +
                      "   l_orderkey, " +
                      "   o_orderdate, " +
                      "   o_shippriority, " +
                      "   revenue " +
                      " ORDER BY " +
                      "   revenue        DESC, " +
                      "   o_orderdate    ASC ",
                      {"orderdate": time.mktime(date1.timetuple()),
                       "shipdate": time.mktime(date2.timetuple()),
                       "segment": segment})
  return result

# Query 4
def query4 (db, region, date):
  result = \
    db.session().run (" MATCH " +
                      " ((r:Region)-[:belongs]->(n:Nation)-[:belongs]->(s:Supplier)-[:has]->(li:LineItem)) , ((r1:Region)-[:belongs]->(n1:Nation)-[:belongs]->(c:Customer)-[:buy]->(o:Order)-[:has]->(li1:LineItem))" +
                      " WHERE " +
                      "   r.name = {region}                 AND" +
                      "   o.orderdate >= {date}             AND" +
                      "   o.orderdate < {date2}                 " +
                      " WITH " +
                      "   n.name                                 AS n_name,  " +
                      "   SUM(li.extendedPrice*(1-li.discount))  AS revenue " +
                      " RETURN " +
                      "   n_name, " +
                      "   revenue " +
                      " ORDER BY " +
                      "   revenue   DESC ",
                      {"date": time.mktime (date.timetuple()),
                       "date2": time.mktime (date.replace (year=date.year + 1).timetuple()),
                       "region": region})
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
  printResult (1, query1 (db, datetime.datetime (2018, 11, 27)))
  # Execute query 2 and print result
  printResult (2, query2 (db, 'EUROPE', 'type2', '5'))
  # Execute query 3 and print result
  printResult (3, query3 (db, datetime.datetime (2020, 12, 27), datetime.datetime (2017, 9, 15), 'MKT5'))
  # Execute query 4 and print result
  printResult (4, query4 (db, "EUROPE", datetime.datetime (2018, 11, 15)))

def main ():
  # Connect to database
  db = GraphDatabase.driver ("bolt://localhost", auth=basic_auth ("neo4j", "neoArnau"))
  # Insert in database
  insertDatabase (db)
  # Create database indexes
  createIndexes (db)
  startTime = time.time();
  # Execute 4 queries in database
  executeQueries (db)
  print ("----------------------------------Time to execute: " + str(((time.time()) - (startTime))) + " ------------------------")


if __name__ == '__main__':
  main()