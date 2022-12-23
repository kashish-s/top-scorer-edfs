import os
import mysql.connector
import pandas as pd

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Shreyansh12"
)
cursor = db.cursor()

def goals(name,goal):
    ans=[]
    cursor.execute('use dsci551')
    for i in range(1,4):
        cursor.execute('select player_name from table_{0}_{1} where goals>{2};'.format(name,i,goal))
        ans.append(cursor.fetchall())
    print(ans)



def xG(name,xG1, xG2):
    ans=[]
    cursor.execute('use dsci551')
    for i in range(1,4):
        cursor.execute('select player_name from table_{0}_{1} where xG>{2} and xG<{3};'.format(name,i,xG1, xG2))
        ans.append(cursor.fetchall())
    print(ans)




def searchGoals(name,goal):
    ans =[]
    cursor.execute('use dsci551')
    for i in range(1,4):
        cursor.execute('select player_name, goals from table_{0}_{1} where goals={2};'.format(name,i,goal))
        ans.append(cursor.fetchall())
    print(ans)

xG('eplGoal',1,2)