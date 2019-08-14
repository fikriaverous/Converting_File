import mysql.connector
import csv
import pymongo
import json

# ========================Mongodb Properties============================

x = pymongo.MongoClient('mongodb://localhost:27017')
db = x['csvtomongodb']
col = db['karakter']

# ========================MySQL Properties============================

dbku = mysql.connector.connect(
    host = 'localhost',
    port = 3306,
    user = 'root',
    passwd = '',
    database = ''
)
kursor = dbku.cursor()

# ========================Read MySQL============================

querydb = '''SELECT * FROM karakter'''

kursor.execute(querydb)

list_mysql = []
for tuples in kursor.fetchall():
    dict_mysql = {}
    dict_mysql['ID'] = tuples[0]
    dict_mysql['Name'] = tuples[1]
    dict_mysql['Age'] = tuples[2]
    list_mysql.append(dict_mysql)

# ========================Read Mongodb============================

data_mongodb = []
for item in list(col.find()):
    dict_mongodb = {}
    for item1 in item.keys():
        if item1 == '_id':
            # dict_mongodb[item1] = f'ObjectId({item[item1]})'
            dict_mongodb[item1] = f'{item[item1]}'
        else:
            dict_mongodb[item1] = item[item1]
    data_mongodb.append(dict_mongodb)

# ========================Mongodb to MySQL============================

list_tuples = []
for item in data_mongodb:
    t = tuple(item.values())
    list_tuples.append(t)

data_tuples = list_tuples

querydb = '''insert into karakter(_id, ID, Name, Age)
values (%s,%s,%s,%s)
'''
kursor.executemany(querydb, data_tuples)
dbku.commit()
print(kursor.rowcount, 'data baru tersimpan.')

# ========================Mongodb to CSV============================

with open('mongodbtocsv.csv', 'w', newline = '') as csvFilefromMongodb:
    tuliscsv = csv.DictWriter(csvFilefromMongodb, fieldnames = ['_id', 'ID', 'Name', 'Age'])
    tuliscsv.writeheader()
    tuliscsv.writerows(data_mongodb)

# ========================Mongodb to json============================

with open('mongodbtojson.json', 'w', newline = '') as jsonFilefromMongodb:
    jsonFilefromMongodb.write(str(data_mongodb).replace("'",'"'))

# ========================MySQL to CSV============================

with open('mysqltocsv.csv', 'w', newline = '') as csvFilefromMySQL:
    tuliscsv = csv.DictWriter(csvFilefromMySQL, fieldnames = ['ID', 'Name', 'Age'])
    tuliscsv.writeheader()
    tuliscsv.writerows(list_mysql)

# ========================MySQL to json===========================

with open('mysqltojson.json', 'w', newline = '') as jsonFilefromMySQL:
    jsonFilefromMySQL.write(str(list_mysql).replace("'",'"'))

# ========================MySQL to mongodb===========================

for dicts in list_mysql:
    tulis = col.insert_one(dicts)

# ========================json to MySQL===========================

with open('mongodbtojson.json') as x:
    data_json = json.load(x)

list_tuples = []
for item in data_json:
    t = tuple(item.values())
    list_tuples.append(t)

data_tuples = list_tuples

querydb = '''insert into karakter(_id, ID, Name, Age)
values (%s,%s,%s,%s)
'''
kursor.executemany(querydb, data_tuples)
dbku.commit()
print(kursor.rowcount, 'data baru tersimpan.')

# ========================csv to MySQL===========================

data_csv = []
with open('mongodbtocsv.csv', 'r', newline = '') as y:
    csvreader = csv.DictReader(y)
    for item in csvreader:
        data_csv.append(dict(item))

list_tuples = []
for item in data_csv:
    t = tuple(item.values())
    list_tuples.append(t)

data_tuples = list_tuples

querydb = '''insert into karakter(_id, ID, Name, Age)
values (%s,%s,%s,%s)
'''
kursor.executemany(querydb, data_tuples)
dbku.commit()
print(kursor.rowcount, 'data baru tersimpan.')

# ========================json to Mongodb===========================

with open('mongodbtojson.json') as x:
    data_json = json.load(x)

for dicts in data_json:
    y = col.insert_one(dicts)

# ========================json to Mongodb===========================

data_csv = []
with open('mongodbtocsv.csv', 'r', newline = '') as y:
    csvreader = csv.DictReader(y)
    for item in csvreader:
        data_csv.append(dict(item))

for dicts in data_csv:
    y = col.insert_one(dicts)

# ========================json to csv===========================

with open('data.json') as x:
    data = json.load(x)

with open('readjsontocsv.csv','w',newline = '') as csvFile:
    bacajson = csv.DictWriter(csvFile, fieldnames = ['Name' , 'Age'])
    bacajson.writeheader()
    bacajson.writerows(data)

# ========================csv to json===========================

data = []
with open('readjsontocsv.csv', 'r', newline = '') as y:
    csvreader = csv.DictReader(y)
    for item in csvreader:
        data.append(dict(item))

with open('readcsvtojson.json', 'w', newline = '') as jsonFile:
    jsonFile.write(str(data).replace("'",'"'))