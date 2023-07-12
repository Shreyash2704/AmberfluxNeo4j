from flask import Flask, jsonify, request
from flask_session import Session
import pandas as pd
from neo4j import GraphDatabase
from Neo4jDB import Neo4jDB
import re

app = Flask(__name__)
app.secret_key = "12344"
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

#Database Credetials
URI = "bolt://neo4j-nlb-e0ad87a85a310b86.elb.us-east-1.amazonaws.com:7687/"
Auth = ("neo4j", "dbadmin@123")
database = "Neo4j"

'''
URI = "bolt://localhost:7687/"
Auth = ("shreyash", "1234") 
database = "rahdemo"
'''


@app.route('/', methods = ['GET', 'POST'])
def home():
    if(request.method == 'GET'):
        data = "hello world"
        resp = jsonify(success=True)
        return resp


@app.route('/healthcheck',methods=['GET'])
def health():
    resp = jsonify(success=True)
    resp.status_code = 200
    return resp

#This route will take a excel file and car,model and problem as input 
#Will call a function which will create a logic tree out of the excel file.

@app.route('/addLogicTree',methods=['POST'])
def addLogicTree():
    file = request.files['file']
    car = request.form["Car"].capitalize()
    model = request.form["Model"].capitalize()
    problem = request.form["Problem"].capitalize()
    df = pd.read_excel(file)
    db = Neo4jDB(URI,Auth,database)
    res = db.CreateLogicTree(df,[car,model,problem])
    print(df)
    print([car,model,problem])
    return jsonify({"message":"Uploaded Successfully","success":True,"response":res})

#This endpoint will give all the nodes for given node id.
@app.route('/getNext',methods=['GET'])
def getNext():
    nodeId = request.args.get('id')
    db = Neo4jDB(URI,Auth,database)
    res = db.getNextProblem(nodeId,"")
    return jsonify(data=res)
 
# This route will return the list of logic tree for a model.
@app.route('/logicTree',methods=['GET'])
def getLogictree() :
    model = request.args.get('model')
    db = Neo4jDB(URI,Auth,database)
    res = db.getLogicTrees(model)
    print(res)
    return jsonify(data=res)
 
#This route will return the first question for a model and logic tree. 
@app.route('/firstQuestion',methods=['GET'])
def getFirstQuestion():
    logicTree = request.args.get('problem').capitalize()
    model = request.args.get('model').capitalize()
    car = request.args.get('car').capitalize()
    
    db = Neo4jDB(URI,Auth,database)
    
    data = {}
    data["Car"] = car
    data["Model"] = model
    data["tree"] = logicTree
    res = db.getLogicTree(data)
    
    return jsonify(data=res)
    

#This route will return the possible problems for a node id.
@app.route('/PossibleCause',methods=['GET'])
def PossibleCause():
    nodeid = request.args.get('id')
    db = Neo4jDB(URI,Auth,database)
    res = db.getPossibleCause(nodeid)
    #print(type(res))
    #sorted_ele = dict(sorted(list(res.items()), key = lambda x: x[1],reverse=True))
    return jsonify(data=res)
              
#This route will return the list of all cars in database.              
@app.route('/Cars',methods=['GET'])
def getCars():
    db = Neo4jDB(URI,Auth,database)
    res = db.Cars()
    return jsonify(data=res)
   
#This route will return all the models for a car.   
@app.route('/Model',methods=['GET'])
def getModel():
    make = request.args.get('make')
    db = Neo4jDB(URI,Auth,database)
    res = db.getModel(make)
    return jsonify(data=res)


if __name__ == '__main__':
    app.run(port=3000,debug=True)