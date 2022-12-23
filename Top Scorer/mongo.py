import pymongo
from pymongo import MongoClient
import pandas as pd
import csv
import numpy as np
import json

client = MongoClient()
print("Success")
db = client['dsci551']
collec = db['namenode']
## for datanode1-> which is epl
collec2 = db['datanode1']

## for datanode1-> which is bundesliga
collec3 = db['datanode2']

## for datanode1-> which is laliga
collec4 = db['datanode3']




def makeDir():
    name = input("Enter name of the directory: ")
    collec.insert_one({'path':'root/{}'.format(name),'name':'{}'.format(name),'fileType':'dir'})



def ls():
    results = collec.find({}, {'path':1,'_id':0})
    for result in results:    
        print(result)



def cat(name):
    df= pd.read_csv('{}.csv'.format(name))
    print(df)



def remove():
    try:
        name = input("Enter name of the directory or file you want to delete: ")
        collec.delete_one({'name':'{}'.format(name)})
        print("removed")

    except:
        print("remove didn't work")





def upload(name,k):
    ## first take the csv and add to df
    datanodeName = []
    j=0
    for i in range(1,k+1):
        datanodeName.append('table_{0}_{1}'.format(name,i))
        j+=1
    
    j=0
    
    for i in range(1, k+1):
        colName = '{}'.format(datanodeName[j])
        col = db[colName]
        col.insert_one({'path':'root/{}'.format(datanodeName[j]),'name':'{}'.format(datanodeName[j]),'fileType':'csv'})
        j+=1


    csv='{}.csv'.format(name)
    df_all=pd.read_csv(csv, index_col=0)
    ## then divide pandas into k parts
    df_split = np.array_split(df_all, k)
    
    
   
    j=0
    for i in range(0,k):
        # print(i)
        print('**************************************')
        # convert into json
        colName = '{}'.format(datanodeName[j])
        j+=1
        col = db[colName]
        records = df_split[i].to_json(orient='index')
        parsed = json.loads(records)

        col.insert_many([parsed])

    #header = ['id','player_name','games','time', 'goals', 'xG', 'assists', 'xA','shots','key_passes' ,'yellow_cards','red_cards' ,'pos' ,'team' ,'npg' ,'npXG' ,'xGChain', 'xGBuildup']
    



def getPartitionLoc(file, part):
    #file will take the name of the file
    # parth will take the number 1,2 or 3
    fullName = 'table_{}_{}'.format(file, part)
    ans = collec.find_one({'name':fullName},{'path':1,'_id':0})
    print(ans)


def readPartition(file,part):
    fullName = 'table_{}_{}'.format(file, part)
    collect = db[fullName]
    answer = collect.find({})
    # cursor.execute('select * from {};'.format(fullName))
    for ans in answer:
        print(ans)




readPartition('eplGoal',3)
