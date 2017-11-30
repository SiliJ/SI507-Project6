# Import statements
import psycopg2
import sys
import psycopg2.extras
import csv
import json
import os.path


# Write code / functions to set up database connection and cursor here.
try:
    conn = psycopg2.connect("dbname='jsili_507projects6' user='anita'")
    print ("successful connecting to the server")
except:
    print("Unable to connect to the database. Check server and credentials.")
    sys.exit(1)

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

cur.execute("DROP TABLE IF EXISTS Sites")
cur.execute("DROP TABLE IF EXISTS States")


# Write code / functions to create tables with the columns you want and all database setup here.
cur.execute("CREATE TABLE IF NOT EXISTS States(ID SERIAL Primary KEY, Name VARCHAR(40) UNIQUE)")
cur.execute("CREATE TABLE IF NOT EXISTS Sites(ID SERIAL Primary KEY, Name VARCHAR(128) UNIQUE,Type VARCHAR(128),State_ID INTEGER references States(ID), Location VARCHAR(255),Description TEXT)")

# Write code / functions to deal with CSV files and insert data into the database here.
fname_arkan='arkansas.csv'
arkansas=open(fname_arkan,'r')
arkan_reader=csv.DictReader(arkansas)

fname_Ca='california.csv'
california=open(fname_Ca,'r')
ca_reader=csv.DictReader(california)

fname_Mi='michigan.csv'
michigan=open(fname_Mi,'r')
mi_reader=csv.DictReader(michigan)
# for row in arkan_reader:
#     print(row,'\n')

# Make sure to commit your database changes with .commit() on the database connection.
def insert_states_data(state_name,conn,cur):
    """Inserts a state name and returns state_name, None if unsuccessful"""
    sql = """INSERT INTO States(Name) VALUES(%s) RETURNING ID"""
    cur.execute(sql,(state_name,))
    # cur.execute("SELECT * FROM States")
    stateID=cur.fetchone()[0]
    # print(state_name)
    # print(stateID)
    # zprint ((ID))
    #print("State name", State_name)
    conn.commit()
    return stateID

def insert_sites_data(park_name,park_type,state_id,park_location,Description,conn,cur):
     """Returns True if succcessful, False if not"""
    #  state_id=insert_states_data(state_name,conn,cur)
    #  print(state_name)
     sql = """INSERT INTO Sites(Name,Type,State_ID,Location,Description) VALUES(%s,%s,%s,%s,%s)"""
     cur.execute(sql,(park_name,park_type,state_id,park_location,Description))
     conn.commit()
     return True

def data_to_dictionMore(datareader):
    dictlist=[]
    for row in datareader:
        dict={}
        dict['Park_Name']=row['NAME']
        dict['Type']=row['TYPE']
        dict['Location']=row['ADDRESS']
        dict['Description']=row['DESCRIPTION']
        dictlist.append(dict)
    #print (dictlist,'\n')
    return (dictlist)

# Write code to be invoked here (e.g. invoking any functions you wrote above)
State_Mi=os.path.splitext(fname_Mi)[0]
Mi_id=insert_states_data(State_Mi,conn,cur)
# cur.execute("SELECT MAX(Name) FROM States"）
# IDM=cur.fetchall()
# print(IDM)
print (Mi_id,"works!")
for diction in data_to_dictionMore(mi_reader):
    res=insert_sites_data(diction["Park_Name"],diction["Type"],Mi_id,diction["Location"],diction["Description"],conn,cur)
print ("*********Sites-data-for-Mi-inserted succcessfully***********")

State_arkan=os.path.splitext(fname_arkan)[0]
ar_id=insert_states_data(State_arkan,conn,cur)
print(ar_id, "works!")
for diction in data_to_dictionMore(arkan_reader):
    res=insert_sites_data(diction["Park_Name"],diction["Type"],ar_id,diction["Location"],diction["Description"],conn,cur)
print ("*********Sites-data-for-ar-inserted succcessfully***********")

State_ca=os.path.splitext(fname_Ca)[0]
ca_id=insert_states_data(State_ca,conn,cur)
print(ca_id, "works!")
for diction in data_to_dictionMore(ca_reader):
    res=insert_sites_data(diction["Park_Name"],diction["Type"],ca_id,diction["Location"],diction["Description"],conn,cur)
print ("*********Sites-data-for-ca-inserted succcessfully***********")


# Write code to make queries and save data in variables here.

cur.execute("SELECT Location FROM Sites")
all_locations=cur.fetchall()
#print (all_locations)

cur.execute("SELECT Name FROM Sites WHERE Description LIKE '%beautiful%'")
beautiful_sites=cur.fetchall()
print (beautiful_sites)

cur.execute("SELECT COUNT(*) FROM Sites WHERE type='National Lakeshore'")
natl_lakeshores=cur.fetchall()
print (natl_lakeshores)

cur.execute("SELECT ID,Name FROM States")
state_situation=cur.fetchall()
def query_ID(lst):
    for item in lst:
        if item[1]=="michigan":
            MID=item[0]
        if item[1]=="arkansas":
           arID=item[0]
    # print (MID,arID)
    return(MID,arID)
state_ID_returned=query_ID(state_situation)
MID=state_ID_returned[0]
print(MID)
arID=state_ID_returned[1]
print(arID)

cur.execute("SELECT Sites.Name FROM Sites INNER JOIN States ON Sites.State_ID=States.ID WHERE States.ID=%s",(MID,))
michigan_names=cur.fetchall()
print(michigan_names)

cur.execute("SELECT COUNT(Name) FROM Sites where Sites.State_ID=%s", (arID,))
total_number_arkansas=cur.fetchall()
print (total_number_arkansas)
cur.close()
conn.close()
# We have not provided any tests, but you could write your own in this file or another file, if you want.
