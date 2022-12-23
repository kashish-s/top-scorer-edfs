from flask import Flask, render_template, request, redirect
import mysql.connector
import hdfs
import requests
import pandas as pd
import numpy as np

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Shreyansh12"
)

cursor = db.cursor()


app = Flask(__name__)

@app.route('/')
def home():
   return render_template('index.html')


@app.route('/templates/mkdir/', methods=["GET", "POST"])
def home1():
    if request.method=="GET":
        return render_template('mkdir.html', done=False)
    elif request.method=="POST":
        name = request.form.get("name")
        cursor.execute("use dsci551")
        cursor.execute("insert into namenode3 (path, name, fileType) values('root/{0}', '{1}', 'dir')".format(name, name))
        db.commit()
        return render_template('mkdir.html', done=True)


@app.route('/templates/ls/')
def home2():
    cursor.execute("use dsci551")
    cursor.execute("select path from namenode3")
    lslist = []
    for x in cursor:
        lslist.append(x)
    return render_template('ls.html',lslist=lslist)


@app.route('/templates/cat/', methods=["GET", "POST"])
def home3():
    if request.method=="GET":
        return render_template('cat.html', done=False)
    
    if request.method=="POST":
        name = request.form.get("name")
        df= pd.read_csv('{}.csv'.format(name))
        return render_template('df.html', df=df.values)


@app.route('/templates/remove/',  methods=["GET", "POST"])
def home4():
    if request.method=="GET":
        return render_template('remove.html', done=False)
    elif request.method=="POST":
        name = request.form.get("name")
        cursor.execute("use dsci551")
        cursor.execute("Delete from namenode3 where name LIKE 'table_{0}__'".format(name))
        db.commit()
        return render_template('remove.html', done=True)

    




@app.route('/templates/upload/', methods=["GET", "POST"])
def home5():
    if request.method=="POST":
        items = request.form.get("numpart").split(",")
        if len(items)!=2:
            print("Error")
        else:
            numpart,dir = items
            cursor.execute("use dsci551")
            name = dir.strip()
            k=numpart.strip()
            k=int(k)
            j=0
    # update namenode
            datanodeName = []
            for i in range(1,k+1):
                datanodeName.append('table_{0}_{1}'.format(name,i))
    
            j=0
            for i in range(1,k+1):
                cursor.execute("insert into namenode3 (path, name, fileType) values('root/{}', '{}', 'csv')".format(datanodeName[j], datanodeName[j]))
                j+=1



    # make k tables
            j=0
            print(datanodeName)
            for i in range(1,k+1):
                # cursor.execute("use dsci551")
                print("create table {} (id varchar(100),player_name varchar(100), games varchar(100), time varchar(100), goals varchar(100), xG varchar(100), assists varchar(100), xA varchar(100), shots varchar(100), key_passes varchar(100), yellow_cards varchar(100), red_cards varchar(100), pos varchar(100), team varchar(100), npg varchar(100), npXG varchar(100), xGChain varchar(100), xGBuildup varchar(100))".format(datanodeName[j]))
                cursor.execute("create table {} (id varchar(100),player_name varchar(100), games varchar(100), time varchar(100), goals varchar(100), xG varchar(100), assists varchar(100), xA varchar(100), shots varchar(100), key_passes varchar(100), yellow_cards varchar(100), red_cards varchar(100), pos varchar(100), team varchar(100), npg varchar(100), npXG varchar(100), xGChain varchar(100), xGBuildup varchar(100))".format(datanodeName[j]))
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
        return render_template('partition.html')
    elif request.method=="GET":
        return render_template('partition.html')


@app.route('/templates/partition/', methods=["GET", "POST"])
def home6():
    if request.method=="POST":
        items = request.form.get("numpart").split(",")
        if len(items)<2:
            print("Error")
        else:
            numpart,dir = items
            cursor.execute("use dsci551")
            file = dir.strip()
            k=numpart.strip()
            k=int(k)
            fullName = 'table_{}_{}'.format(file, k)
            cursor.execute('select path from namenode3 where name ="{}";'.format(fullName))
            part = cursor.fetchall()
            lslist=[]
            lslist.append(part)
        return render_template('partition.html',lslist=lslist)
    elif request.method=="GET":
        return render_template('partition.html')


@app.route('/templates/read/', methods=["GET", "POST"])
def home7():
    if request.method=="GET":
        return render_template('read.html')
    
    if request.method=="POST":
        items = request.form.get("numpart").split(",")
        if len(items)<2:
            print("Error")
        else:
            numpart,dir = items
            file = dir.strip()
            k=numpart.strip()
            k=int(k)

            fullName = 'table_{}_{}'.format(file, k)
            cursor.execute('use dsci551')
            cursor.execute('select * from {};'.format(fullName))
            lslist=[]
            for x in cursor:
                lslist.append(x)
            return render_template('df.html', df=lslist)



@app.route('/templates/search/',methods=["GET","POST"])
def home8():
    if request.method=="GET":
        return render_template('search.html')
    elif request.method=="POST":
        cursor.execute('use dsci551')
        que = request.form.get("query").split(',')
        if que[0]=='1':
            ans=[]
            name = que[1]
            goal = que[2]
            for i in range(1,4):
                cursor.execute('select player_name from table_{0}_{1} where goals>{2};'.format(name,i,goal))
                ans.append(cursor.fetchall())
            return render_template('search.html',lslist=ans)   
        if que[0]=='2':
            ans=[]
            name = que[1]
            xG1 = que[2]
            xG2 = que[3]
            for i in range(1,4):
                cursor.execute('select player_name from table_{0}_{1} where xG>{2} and xG<{3};'.format(name,i,xG1, xG2))
                ans.append(cursor.fetchall())
            return render_template('search.html',lslist=ans)   

        if que[0]=='3':
            ans =[]
            name = que[1]
            goal = que[2]
            for i in range(1,4):
                cursor.execute('select player_name, goals from table_{0}_{1} where goals={2};'.format(name,i,goal))
                ans.append(cursor.fetchall())
            return render_template('search.html',lslist=ans)   

            

if __name__ == '__main__':
   app.run()





# @app.route("/")
# def main():
#     # cars = []
#     # cursor.execute("use dsci551")
#     # cursor.execute("SELECT * FROM namenode3")
#     # for row in cursor.fetchall():
#     #     cars.append({"id": row[0], "path": row[1], "name": row[2], "fileType": row[3]})
#     # db.commit()
#     # cursor.close()

#     return render_template("index.html")




#  "create table dummy (id varchar(100),player_name varchar(100), games varchar(100), time varchar(100), goals varchar(100), xG varchar(100), assists varchar(100), xA varchar(100), shots varchar(100), key_passes varchar(100), yellow_cards varchar(100), red_cards varchar(100), pos varchar(100), team varchar(100), npg varchar(100), npXG varchar(100), xGChain varchar(100), xGBuildup varchar(100))"
