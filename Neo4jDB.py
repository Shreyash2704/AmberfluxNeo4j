import json
import os
import pandas as pd
import csv
from neo4j import GraphDatabase
from collections import OrderedDict

class Neo4jDB:
    URI = ""
    AUTH = ""
    driver = ""
    session = ""

    def __init__(self,URI,AUTH,db):
        self.URI = URI
        self.AUTH = AUTH
        self.driver = GraphDatabase.driver(URI, auth=AUTH)
        self.session = self.driver.session(database=db)
        
    #This method will check if node is present or not. 
    #There are 4 types of nodes, it will return the no.of nodes of pesent in db. 
    #If no node is there it will return zero.
    def check_if_node_present(self,tx,node):
        if node["type"] == "Car":
            result = tx.run("Match (n:Car) where n.Name=$name return n",name=node["Name"])
            return len([ele["n"] for ele in result])
            
        elif node["type"] == "Model":
            result = tx.run("Match (n:Model) where n.Name=$name return n",name=node["Name"])
            return len([ele["n"] for ele in result])
            
        elif node["type"] == "Possible_Problem":
            result = tx.run("Match (n:Possible_Problem) where n.name=$name return n",name=node["name"])
            return len([ele["n"] for ele in result])
        else:
            result = tx.run("Match (n:Problem) where n.desc=$name return n",name=node["desc"])
            return len([ele["n"] for ele in result])
    
    #This method will check if a relationship is present between two nodes.    
    def check_if_relationship_present(self,tx,data):
        #if relation is of type Leads_to then the relationship will have weight and name as attribute.
        if data[1]["type"] == "Leads_to":
            res = tx.run("Match (n) where id(n)=$node1 Match (m) where id(m)=$node2 Match p = (n)-[r:"+str(data[1]["type"])+"]->(m) where r.relation=$rel AND r.weight=$weight  return p",node1=data[0],node2=data[2],rel=data[1]["name"],weight=data[1]["weight"])
            ans = [ele["p"] for ele in res]
            return len(ans)
        #other wise, the relation will only have name as attribute
        else:
            res = tx.run("Match (n) where id(n)=$node1 Match (m) where id(m)=$node2 Match p = (n)-[r:"+str(data[1]["type"])+"]->(m) where r.relation=$rel  return p",node1=data[0],node2=data[2],rel=data[1]["name"])
            ans = [ele["p"] for ele in res]
            return len(ans)
        
    #This method will check if there is any model has a relationship with any question and relationshp named being logictree name.
    #Returns 0 if logictree exist 
    #Returns 1 if logictree do not exist 
    #Returns 2 if car_make or model do not exist.
    def isLogicTreePresent(self,tx,data):
        car = data["Car"].capitalize()
        model = data["Model"].capitalize()
        tree = data["tree"].capitalize()
        
        
        if len([x["id"] for x in tx.run("Match (n:Car) where n.Name=$car return id(n) as id",car=car)]) != 0:
            if len([ele["id"] for ele in tx.run("Match (n:Car) where n.Name=$car Match (m:Model) where m.Name=$model Match (n)-[r:Model]->(m) return id(m) as id",car=car,model=model)]) != 0:
                ans = tx.run("Match (n:Model) where n.Name=$name  Match (n)-[r]->(m) where r.relation=$relation  return id(m) as id,r.relation as rel",name=model,relation=tree)
                temp = [e["id"] for e in ans]
                if len(temp) != 0:
                    return 0
                else:
                    return 1
            else:
                return 2
        else:
            return 2
       

    #The method will return the id of a node.
    #Node being Car,Model,Problem,Possible_Problem
    def getNodeId(self,tx,node):
        #print(node)
        if node["type"] == "Car":
            res = tx.run('Match (n:Car) where n.Name=$name return id(n) as id',name=node["Name"])
            return [ele["id"] for ele in res]
        if node["type"] == "Model":
            res = tx.run('Match (n:Model) where n.Name=$name return id(n) as id',name=node["Name"])
            return [ele["id"] for ele in res]
        if node["type"] == "Problem":
            res = tx.run('Match (n:Problem) where n.desc=$desc return id(n) as id',desc=node["desc"])
            return [ele["id"] for ele in res]
        if node["type"] == "Possible_Problem":
            res = tx.run('Match (n:Possible_Problem) where n.name=$name return id(n) as id',name=node["name"])
            return [ele["id"] for ele in res]
        else:
            return None
    
    #This method will traverse the whole tree using depth first search.
    #Although this method is in use due to time complexity issue.
    #It will return all list of all node ids present in logic tree.
    #Logic tree basically kind of graph start with car, -> model -> question and then possible_problems.    
    def traverse(self,tx,node,l=[]): 
        l.append(node)
        result = tx.run("Match (n) where id(n)=$ids Match (n)-[r]->(m) return id(m) as id,r.relation as rel",ids=node)
        nodes = [[res["id"],res["rel"]] for res in result]
        if len(nodes) == 0:
            #print("exit")
            return 
        #print(nodes)
        for n in nodes:
            if n not in l:
                l.append(n[0])
                self.traverse(tx,n[0])
        
        return l 

    def traverse_(self,node):
        res = self.session.execute_read(self.traverse,node)
        print(set(res))
    
    #The add_nodes function will add a node in database. 
    #Returns the node id.
    def add_nodes(self,tx,node):
        if node["type"] == "Car":
            result = tx.run("Create (n:Car) SET n.Name=$name SET n.type=$type_ Return id(n) as id ",name=node["Name"],type_=node["type"])
            #print("Car Created")
            return result.single()[0]
            
        elif node["type"] == "Model":
            result = tx.run("Create (n:Model) SET n.Name=$name SET n.type=$type_ Return id(n) as id  ",name=node["Name"],type_=node["type"])
            #print("Model Created")
            return result.single()[0]
            
        elif node["type"] == "Possible_Problem":
            result = tx.run("Create (n:Possible_Problem) set n.name=$name SET n.type=$type_ Return id(n) as id ",name=node["name"],type_=node["type"])
            #print("PP Created")
            return result.single()[0]
        else:
            result = tx.run("Create (n:Problem) set n.desc=$desc SET n.type=$type_ Return id(n) as id ",desc=node["desc"],type_=node["type"])
            #print("Problem Created")
            return result.single()[0]
           
    #This method take two nodes, and a relation as input and creates a relationship between them.
    #node1 -> Node no.1 (eg.Car,Model,Problem)
    #node2 -> Relationship (eg. model,labels(yes,no,...),leads_to)
    #node3 -> Node no.2 (eg.Model,Problem,Possible_Problem)
    def add_relationship(self,tx,data):
        print(data)
        node1 = data[0]
        node2 = data[1]
        node3 = data[2]
        rel_type = node2["type"]
        relation = node2["name"] 
        
        
        #if weight is in node2, then node3 or Node no.2 is Possible_Problem and Node no.1 is Problem.
        #While creating new relationship between problem and possible_problems, an addition property of relation i.e weight is added along with name.
        if "weight" in node2.keys():
            weight = node2["weight"]
            result = tx.run("Match (n) where id(n) = $node1 Match (m) where id(m) = $node3 Create (n)-[r:"+str(rel_type)+"]->(m) SET r.relation=$relation SET r.weight=$weight Return 'Relation Created'",node1=node1,node3=node3,relation=relation,weight=weight) 
            
            return result.single()[0]
        
        #while creating other relationship, they all have same property i.e (name).
        else:
            result = tx.run("Match (n) where id(n) = $node1 Match (m) where id(m) = $node3 Create (n)-[r:"+str(rel_type)+"]->(m) SET r.relation=$relation Return 'Relation Created'",node1=node1,node3=node3,relation=relation)
            
            return result.single()[0]


    #This method will take node no.1 and node no.2 and relation as input.
    #First it will check if each node is present or not. 
    #If not it will create the node and get their ids.
    #If present then it will fetch there ids.
    #Then it will check if they have a relationship, if not it will be created otherwise nothing will happen.
    #For eg. Node no.1 (type:Car, Name:Honda) Node no.2 (type:Model,Name:City) Relationship (type:Problem,name:logic tree name)
    
    def add_logic_tree(self,data,Model):
        node_set = []
        res = self.session.execute_read(self.isLogicTreePresent,Model)
        #res == 2, means node 1 and node 2 do not exist in databse.
        if res == 2:
            node1,node2 = None,None
            for i in data:
                #print(i)
                node1 = self.CreateNode(i[0],node_set)
                node_set.append(node1)
                node2 = self.CreateNode(i[2],node_set)
                node_set.append(node2)
                if self.session.execute_read(self.check_if_relationship_present,[node1,i[1],node2]) == 0:
                    res = self.session.execute_write(self.add_relationship,[node1,i[1],node2])
                else:
                    print("Relation already present ",[node1,i[1],node2])
                #print(res)
            print("Tree Created")
            return "Tree Created Successfully"
        #res ==1 means car and model exist but node logic tree is present.
        elif res == 1:
            
            model = {}
            model["type"] = "Model"
            model["Name"] = Model["Model"]
            
            node = self.session.execute_read(self.getNodeId,model)
            #node_set = self.session.execute_read(self.traverse,node[0])
            #if node_set is None:
            node_set = []
            
            #print("set:",node_set)
            node1,node2 = None,None
            for i in data:
                #print(i)
                node1 = self.CreateNode(i[0],node_set)
                node_set.append(node1)
                node2 = self.CreateNode(i[2],node_set)
                node_set.append(node2)
                if self.session.execute_read(self.check_if_relationship_present,[node1,i[1],node2]) == 0:
                    res = self.session.execute_write(self.add_relationship,[node1,i[1],node2])
                else:
                    print("Relation already present ",[node1,i[1],node2])
            print("Tree Created")
            return "Tree Created Successfully"            
        else:
            print("Already Exist")
            return "Logic Tree Already Exist."

    #This method will create the nodes if they don't exist.
    #Now there may be case if for different logic tree same Problem Node i.e(Question) would be there. 
    #This method will check if node type is (Car,Model or Possible_Problem) then no duplicates node will be created and existing node id will be returned.
    #But if node type is Problem then a duplicate node will be created.(It will reduce the complexity of graph and avoid formation of cyclic graph structure.)
    
    def CreateNode(self,node,node_set):
        node1 = None
        #print(node)
        if self.session.execute_read(self.check_if_node_present,node) == 0:
            node1 = self.session.execute_write(self.add_nodes,node)
            return node1
        
        ele = self.session.execute_read(self.getNodeId,node)
        if node["type"] == "Problem":
            for e in ele:
                if e in node_set:
                    return e
            
            node1 = self.session.execute_write(self.add_nodes,node)
            return node1
        
        
        return ele[0] 
        
        
    
    #This method will take a dataframe as input and read each rows and creates relationship between them.
    def CreateLogicTree(self,df,data):

        relations = []
        nodes = []
        car = []
        problem = []
        pp = []
        model = []
        all_pp = []
        for index, row in df.iterrows():
            node1 = row["Node1"].split(":",1)[0]
            name1 = row["Node1"].split(":",1)[1].strip().capitalize()
            node2 = row["Node2"].split(":",1)[0]
            name2 = row["Node2"].split(":",1)[1].strip().capitalize()
            
            
            if node1 == "Car" and node2 == "Model":
                n1 = {}
                n2 = {}
                r = {}
                n1["type"] = node1
                n1["Name"] = name1
                n2["type"] = node2 
                n2["Name"] = name2
                r["type"] = "Model"
                r["name"] = row['Relationship'].capitalize()
                relations.append([n1,r,n2]) 
            
            if node1 == "Model" and node2 == "Problem":
                n1 = {}
                n2 = {}
                r = {}
                n1["type"] = node1
                n1["Name"] = name1 
                n2["type"] = node2 
                n2["desc"] = name2 
                r["type"] = "Problem"
                r["name"] = row['Relationship'].capitalize()
                relations.append([n1,r,n2]) 
            
            if node1 == "Car" and node2 == "Problem":
                n1 = {}
                n2 = {}
                r = {}
                n1["type"] = node1
                n1["Name"] = name1 
                n2["type"] = node2 
                n2["desc"] = name2 
                r["type"] = "Problem"
                r["name"] = row['Relationship'].capitalize()
                relations.append([n1,r,n2])
            
            if node1 == "Problem" and node2 == "Problem":
                n1 = {}
                n2 = {}
                r = {}
                n1["type"] = node1
                n1["desc"] = name1.replace("\n"," ")
                n2["type"] = node2 
                n2["desc"] = name2.replace("\n"," ")
                r["type"] = "Problem"
                r["name"] = row['Relationship'].capitalize()
                relations.append([n1,r,n2]) 
                
              


            if node1 == "Problem" and node2 == "Possible_Problem":
                arr = name2.split('\n')
                for e in range(len(arr)):
                    if "%" not in arr[e]:
                        arr[e] = "% "+arr[e]
                for e in arr:
                    n1 = {}
                    n2 = {}
                    r = {}
                    n1["type"] = node1
                    n1["desc"] = name1.replace("\n","")
                    n2["type"] = node2 
                    n2["name"] = e.split("%")[1].strip().capitalize()
                    r["type"] = "Leads_to"
                    r["weight"] = str(e.split("%")[0])
                    r["name"] = row['Relationship']
                    relations.append([n1,r,n2]) 

            if node1 == "Model" and node2 == "Possible_Problem":
                arr = name2.split('\n')
                for e in range(len(arr)):
                    if "%" not in arr[e]:
                        arr[e] = "% "+arr[e]
                for e in arr:
                    n1 = {}
                    n2 = {}
                    r = {}
                    n1["type"] = node1
                    n1["Name"] = name1 
                    n2["type"] = node2 
                    n2["name"] = e.split("%")[1].strip().capitalize()
                    r["type"] = "Leads_to"
                    r["weight"] = str(e.split("%")[0])
                    r["name"] = row['Relationship']
                    relations.append([n1,r,n2])            
            
            
            
            
            if node1 == "Car" and name1 not in car:
                ele = {}
                ele["type"] = node1
                ele["Name"] = name1
                nodes.append(ele)
                car.append(name1)
                
            if node1 == "Model" and name1 not in model:
                ele = {}
                ele["type"] = node1
                ele["Name"] = name1
                nodes.append(ele)
                model.append(name1)
                
            if node1 == "Possible_Problem":
                arr = name1.split('\n')
                for e in range(len(arr)):
                    if "%" not in arr[e]:
                        arr[e] = "% "+arr[e]
                for e in arr:
                    if e.split("%")[1].strip().capitalize() not in pp:
                        ele = {}
                        ele["type"] = node1
                        ele["name"] = e.split("%")[1].strip().capitalize()
                        nodes.append(ele)
                        pp.append(e.split("%")[1].strip().capitalize())
                
                
            if node1 == "Problem" and name1 not in problem:
                ele = {}
                ele["type"] = node1
                ele["desc"] = name1.replace("\n","")
                nodes.append(ele)
                problem.append(name1)
                
            if node2 == "Car" and name2 not in car:
                ele = {}
                ele["type"] = node2
                ele["Name"] = name2
                nodes.append(ele)
                car.append(name1)
            if node2 == "Model" and name2 not in model:
                ele = {}
                ele["type"] = node2
                ele["Name"] = name2
                nodes.append(ele)
                model.append(name2)
                
                
            if node2 == "Possible_Problem":
                arr = name2.split('\n')
                for e in range(len(arr)):
                    if "%" not in arr[e]:
                        arr[e] = "% "+arr[e]
                for e in arr:
                    all_pp.append(e.split("%")[1].strip().capitalize())
                    if e.split("%")[1].strip().capitalize() not in pp:
                        ele = {}
                        ele["type"] = node2
                        ele["name"] = e.split("%")[1].strip().capitalize()
                        nodes.append(ele)
                        pp.append(e.split("%")[1].strip().capitalize())
                
            if node2 == "Problem" and name2 not in problem:
                ele = {}
                ele["type"] = node2
                ele["desc"] = name2.replace("\n","")
                nodes.append(ele)
                problem.append(name2)



        res_ = self.add_logic_tree(relations,{"Car":data[0],"Model":data[1],"tree":data[2]})
        
        print("Done")
        return res_
    
    #---------------------------------------
    #Purpose:
    #This method will take model name and logic tree name as input and return the first question.
    
    #Why,What it does
    #Basically the Graph consist of Nodes and Edges. And Graph Starts from Car Node -> Model Node 
    #The Model Node have multiple edges (which represnt logic tree names)
    #Now the Model Node with logic tree named edge will give us a Question Node in response.
    
    #Attributes:
    #tx.run() => returns a list which matches query.
    #List has elements => id , Relation_type( eg. problem), Node_type(eg.Problem), Node Description(eg.Questions to be ask)
    #In return of the function it will return a dictionary containing id,question.
    
    #Query Explaination:
    #Match (n:Model) where n.Name=$model  => it gets the model node and store it in Variable 'n'.
    #Match (n)-[r]->(m) where r.relation=$rel return id(m) as id,m.type as node_type,m.desc as node_desc,r.relation as rel
    #The above query will map Node 'n' with relation 'r' and find next node 'm' and returns 'm' node details as m.id,m.desc ... .
    
    def getLogicTree_(self,tx,data):
        res = tx.run("Match (n:Model) where n.Name=$model Match (n)-[r]->(m) where r.relation=$rel return id(m) as id,m.type as node_type,m.desc as node_desc,r.relation as rel",model=data["Model"],rel=data["tree"]) 
        ans = [[ele["id"],ele["rel"],ele["node_type"],ele["node_desc"]] for ele in res][0]
        return {"id":ans[0],"Question":ans[3]}
      
    def getLogicTree(self,data):
        '''
        data = {}
        data["Car"] = "Dodge"
        data["Model"] = "Grand caravan"
        data["tree"] = "No heat from heater"
        '''
        res = self.session.execute_read(self.isLogicTreePresent,data)
        if res == 0:
            res = self.session.execute_read(self.getLogicTree_,data)
            qid = res["id"]
            res_ = self.getPossibleCause(qid)
            res["Possible_Problem"] = res_
            return res
        else:
            return {"message":"No Logic Tree Present"}
    #----------------------------------------
    
    #----------------------------------------
    #This method will get all the possible causes for a question id.
    
    #Query Explaination
    #Match (n) where id(n)=$nodeId => this query will get Node and store it in 'n'.
    #Match (n)-[r]->(m) where r.relation=$pp return id(m) as id,m.type as node_type ...
    #The above query will map node 'n' and r with relation name "Possible_Problem" and would return list of all nodes 'm'.
    #The 'm' node have => possible_problems node details i.e id,name,weight
    def getPossibleCause_(self,tx,node_id):
        res = tx.run("Match (n) where id(n)=$nodeId Match (n)-[r]->(m) where r.relation=$pp return id(m) as id,m.type as node_type,m.name as node_pp,r.relation as rel,r.weight as weight",nodeId=int(node_id),pp="Possible_Problem")
        arr = [{"id":ele["id"],"name":ele["node_pp"],"weight":ele["weight"]} for ele in res]
        ans = {}
        for i in arr:
            ans[i["name"]] = int(i["weight"])
        sorted_ele = OrderedDict(sorted(list(ans.items()), key = lambda x: x[1],reverse=True))
        print(sorted_ele)
        return sorted_ele
        
        
    def getPossibleCause(self,nodeid):
        res = self.session.execute_read(self.getPossibleCause_,nodeid)  
        return res        
        
    #----------------------------------------
    #this method will get next leading nodes for a nodes id.
    #The leading node can be a Question/Problem Node and Possible_Problem Node.
    #getNextProblem_ function will return list all possible result nodes.
    #The getNextProblem calls the getNextProblem_ and filtered out the possible_problems nodes and problem/Question nodes.
    #
    #Query Explaination:
    #Match (n) where id(n)=$nodeId => searchs the node and store it in 'n'.
    #Match (n)-[r]->(m) return id(m) as id,m.type as node_type,m.desc as node_desc,m.name as node_pp,r.relation as rel,r.weight as weight
    #The above query will return list of object such as :-
    #Result=> list of [{id:"",node_type:"",node_desc:"",node_pp:"",rel:"",weight:""},...]
    #If Node type is Problem then result object will be {id:101,node_type:Problem,node_desc:Question...,node_pp:None/Null,rel:Problem,weight:None}
    #for Problem Node, node_pp,weight will be null.
    #for Possible_Problem Node, node_desc will be Null.
    #Possible_Problem nodes i.e => Possible_Causes or final causes.
    #eg. {id:101,node_type:Possible_Problem,node_desc:None/Null,node_pp:possible_problems/high/low/,rel:possible_problems,weight:20}
    
    def getNextProblem_(self,tx,data):
        node_id = data[0]
        res = tx.run("Match (n) where id(n)=$nodeId Match (n)-[r]->(m) return id(m) as id,m.type as node_type,m.desc as node_desc,m.name as node_pp,r.relation as rel,r.weight as weight",nodeId=node_id)
        return [[ele["id"],ele["rel"],ele["node_type"],ele["node_desc"],ele["node_pp"],ele["weight"]] for ele in res]
    #this method will call the getNextProblem_ methods and restructurize the output and returns  the result.
    def getNextProblem(self,nodeid,rel):
        res = self.session.execute_read(self.getNextProblem_,[int(nodeid)])
        arr = []
        for ele in res:
            e = {}
            if ele[2] == "Possible_Problem" and ele[3] is None:
                e["id"] = ele[0]
                e["Type"] = "Possible_Problem"
                e["Node"] = ele[4]
                e["Relation"] = ele[1]
                e["weight"] = ele[5]
            else:
                e["id"] = ele[0]
                e["Type"] = "Problem"
                e["Node"] = ele[3]
                e["Relation"] = ele[1]
            arr.append(e)
        ans = {} 
        
        for ele in arr:
            
            if ele["Relation"] != "Possible_Problem":    
                if ele["Relation"] in ans.keys():
                    e = {}
                    if ele["Type"] == "Possible_Problem":
                        e["weight"] = ele["weight"]
                    e["Node"] = ele["Node"]
                    e["id"] = ele["id"]
                    e["Type"] = ele["Type"]
                    ans[ele["Relation"]].append(e)
                else:
                    ans[ele["Relation"]] = []
                    e = {}
                    if ele["Type"] == "Possible_Problem":
                        e["weight"] = ele["weight"]
                    e["Node"] = ele["Node"]
                    e["id"] = ele["id"]
                    e["Type"] = ele["Type"]
                    ans[ele["Relation"]].append(e)
               
        print(ans)
        for k,v in ans.items():
           
            ele = {}
            
            for e in v:
          
                #pp
                if e["Type"] == "Possible_Problem":
                    #print(ele,"Possible_Problem" in ele.keys())
                    if "Possible_Problem" in ele.keys():
                        ele[e["Type"]].append({"id":e["id"],"Name":e["Node"],"Weigth":e["weight"]})
                    else:
                        ele[e["Type"]] = [{"id":e["id"],"Name":e["Node"],"Weigth":e["weight"]}]
                #p
                if e["Type"] == "Problem":
                    if "Problem" in ele.keys():
                        ele[e["Type"]].append({"id":e["id"],"Name":e["Node"]})
                    else:
                        ele[e["Type"]] = [{"id":e["id"],"Name":e["Node"]}]
                #print(ele)
            #print("---------")
            ans[k] = ele
                
        #print(ans)
        for k,v in ans.items():
            for key,value in v.items():
                ele = {}
                if key == "Possible_Problem":
                    for e in value:
                        ele[e["Name"]] = int(e["Weigth"])
                    sorted_ele = dict(sorted(list(ele.items()), key = lambda x: x[1]))
                    print("-##--")
                    print(sorted_ele)
                    print("-----")
                    v[key] = sorted_ele
                
        
        return ans
     
    #This method will give all the logic tree name associated with a model. 
    def getLogicTrees_(self,tx,model):
        print(model)
        res = tx.run("Match (n:Model) where n.Name=$model Match (n)-[r]->() return r.relation as rel",model=model)
        ans = [ele["rel"] for ele in res]
        return ans
        
    def getLogicTrees(self,model):
        res = self.session.execute_read(self.getLogicTrees_,model)
        return res
    #This method will return all the Cars present in db.    
    def get_all_car_make(self,tx):
        res = tx.run("Match (n:Car) return n.Name as Name")
        ans = [ele["Name"] for ele in res]
        return ans
    def Cars(self):
        res = self.session.execute_read(self.get_all_car_make)
        return res 
    #This method will give all the models associated with a Car Make.    
    def get_models_for_carMake(self,tx,make):
        res = tx.run("Match (n:Car) where n.Name=$make Match (n)-[:Model]->(m) return m.Name as Name",make=make)
        ans = [ele["Name"] for ele in res]
        return ans
        
    def getModel(self,make):
        res = self.session.execute_read(self.get_models_for_carMake,make)
        return res
        
        




                
                