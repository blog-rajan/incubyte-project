import pymysql
from pymysql.constants import CLIENT

# variables
file_name = "data"
file_ext = "data"
# default date format
date_format = "YYYYMMDD"

# db connection
conn = {
   "host": "localhost",
   "user": "admin",
   "password": "Test@1234",
   "port": 3306,
   "database": "test",
   "client_flag": CLIENT.MULTI_STATEMENTS
}
db = pymysql.connect(**conn)
cur = db.cursor()

# file name
file = file_name + "."  + file_ext
with open (file, "r") as myfile:
    file_data = myfile.read().splitlines()

# data formatter
def date_formatter(date, date_format=date_format):
   date=str(date)
   if date_format == "YYYYMMDD":
      return str(date[0:4]) + "-" + str(date[4:6]) + "-" + str(date[6:8])
   elif date_format == "DDMMYYYY":
      return str(date[4:8]) + "-" + str(date[2:4]) + "-" + str(date[0:2])
   else:
      print("Invalid date")
      return None

with open ("data.data", "r") as myfile:
    data = myfile.read().splitlines()

# list of countries
country = {}
for d in data:
   x = d.split('|')
   if(x[1]=="D"):
      country[x[9]]=x[9]

# create country tables
sql = ''
for c in country:
   sql += 'drop table if exists patients_'+ c+';'
   sql += 'create table patients_' + c + ''' (
      name varchar(255) not null primary key,
      cust_i varchar(18) not null,
      open_dt date not null,
      consl_dt date,
      vac_id char(5),
      dr_name varchar(255),
      state char(5),
      dob date,
      flag char(1)
      );'''

try:
   cur.execute(sql)
   db.commit()
except:
   db.rollback()
   print("fail")


# insert data
for d in data:
   x = d.split('|')
   name = x[2]
   cust_i = x[3]
   open_dt = date_formatter(x[4])
   consl_dt = date_formatter(x[5])
   vac_id = x[6]
   dr_name = x[7]
   state = x[8]
   dob = date_formatter(x[10],"DDMMYYYY")
   flag = x[11]
   if(x[1]== "D"):
      sql = 'insert into patients_' + x[9] + ''' (name, cust_i, open_dt, consl_dt, vac_id, dr_name, state, dob, flag)
      values ("''' + name + '''", "''' + cust_i + '''", "''' + open_dt + '''", "''' + consl_dt + '''", "''' + vac_id + '''", "''' + dr_name + '''", "''' + state + '''", "''' + dob + '''", "''' + flag + '''");'''
      try:
         cur.execute(sql)
         db.commit()
         print("success")
      except:
         db.rollback()
         print("fail")
db.close()
