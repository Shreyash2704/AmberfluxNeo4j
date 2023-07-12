import os
import pandas as pd
from neo4j import GraphDatabase
from Neo4jDB import Neo4jDB



URI = "bolt://localhost:7687/"
Auth = ("shreyash", "1234")
database = "demo2"

'''
URI = "bolt://neo4j-nlb-e0ad87a85a310b86.elb.us-east-1.amazonaws.com:7687/"
Auth = ("neo4j", "dbadmin@123")
database = "Neo4j"
'''
path2 = "Combined Finalize logic tree"
path1 = "Finalize logic tree"

dir_ = os.getcwd()

path = os.path.join(dir_,path1)


files = os.listdir(path)
files = sorted(files)
db = Neo4jDB(URI,Auth,database)

for file in files:
    df = pd.read_excel(os.path.join(path,file))
    name = file.split(".")[0].split("_")
    data = [ele.capitalize() for ele in name]
    data[2] = data[2].replace("--","/")
    print(name)
    print(df["Relationship"][1])
    db.CreateLogicTree(df,data)
    print(file," Done")
  
