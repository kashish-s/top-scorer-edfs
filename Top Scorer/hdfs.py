## This file contains hdfs commands like mkdor, cat, and ls

import os
import mysql.connector
import pandas as pd
import numpy as np

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Shreyansh12"
)
cursor = db.cursor()

# this function should create a directory and also store its meta data in the namenode table
def makeDir():
    name = input("Enter name of the directory: ")
    os.makedirs(name)
    cursor.execute("use dsci551")
    cursor.execute("insert into namenode3 (path, name, fileType) values('root/{0}', '{1}', 'dir')".format(name, name))

    db.commit()


# this func lists all the values in path column of the namenode table
def ls():
    cursor.execute("use dsci551")
    cursor.execute("select path from namenode3")
    for x in cursor:
        print(x)

## TEST THIS
def cat(name):
    cursor.execute("use dsci551")
    cursor.execute("select fileType from namenode3 where name = 'table_{}_1'".format(name))
    typeFile = cursor.fetchall()
    if typeFile[0][0] =='csv':
        df= pd.read_csv('{}.csv'.format(name))
        print(df)
    else:
        with open("{0}.csv".format(name), encoding = 'utf-8') as f:
            text=f.read()
            print(text)
        

# def cat2(name):
#     with open("{0}".format(name), encoding = 'utf-8') as f:
#         text=f.read()
#         print(text)


def remove():
    try:
        name = input("Enter name of the directory or file you want to delete: ")
        cursor.execute("use dsci551")
        cursor.execute("Delete from namenode3 where name LIKE'table_{0}__'".format(name))
        # cursor.execute("Delete from namenode3 where path='root/{0}_1'".format(name))
        # cursor.execute("Delete from namenode3 where path='root/{0}_1'".format(name))
        db.commit()
        print("removed")

    except:
        print("remove didn't work")





def upload(name,k):
    cursor.execute("use dsci551")
    j=0
    # update namenode
    datanodeName = []
    for i in range(1,k+1):
        datanodeName.append('table_{0}_{1}'.format(name,i))
        j+=1
    
    j=0
    for i in range(1,k+1):
        cursor.execute("insert into namenode3 (path, name, fileType) values('root/{}', '{}', 'csv')".format(datanodeName[j], datanodeName[j]))
        j+=1



    # make k tables
    j=0
    for i in range(1,k+1):
        
        cursor.execute("use dsci551")
        cursor.execute("create table {}(id varchar(100),player_name varchar(100), games varchar(100), time varchar(100), goals varchar(100), xG varchar(100), assists varchar(100), xA varchar(100), shots varchar(100), key_passes varchar(100), yellow_cards varchar(100), red_cards varchar(100), pos varchar(100), team varchar(100), npg varchar(100), npXG varchar(100), xGChain varchar(100), xGBuildup varchar(100))".format(datanodeName[j]))
        db.commit()
        j+=1

    # make dataframes
    csv='{}.csv'.format(name)
    df_all=pd.read_csv(csv, index_col=0)
    df_split = np.array_split(df_all, k)
    #store data/k in each table
    j=0
    # for i in range(1,k+1):
    for i in range(0,k):
        dfList = df_split[i].values.tolist()
        cursor.execute("use dsci551")
        sql = "INSERT INTO {} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(datanodeName[i])
        cursor.executemany(sql,dfList)        
    db.commit()



def getPartitionLoc(file, part):
    #file will take the name of the file
    # parth will take the number 1,2 or 3
    fullName = 'table_{}_{}'.format(file, part)
    cursor.execute('use dsci551')
    cursor.execute('select path from namenode3 where name ="{}";'.format(fullName))
    part = cursor.fetchall()
    print(part[0][0])


def readPartition(file,part):
    fullName = 'table_{}_{}'.format(file, part)
    cursor.execute('use dsci551')
    cursor.execute('select * from {};'.format(fullName))
    print(cursor.fetchall())
    


readPartition('eplGoal',3)