# python -m pip install mysql-connector-python
# https://www.w3schools.com/python/python_mysql_getstarted.asp

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  port="3306",
  user="debezium",
  password="dbz",
  database="dataops"
)

mycursor = mydb.cursor()

mycursor.execute("SHOW DATABASES;")

for x in mycursor:
  print(x)

# ########## INSERT ##############
# sql = "INSERT INTO example (customerId, customerFName, customerLName, customerCity) VALUES (%s, %s, %s, %s)"
# val = (200, "Elif", "Aktas", "Highway 21")
# mycursor.execute(sql, val)
# mydb.commit()
# print(mycursor.rowcount, "record inserted.")

# ######### UPDATE ##############
# sql = "UPDATE example SET customerCity = 'Ocean Drive 6' WHERE customerId = 200"
# mycursor.execute(sql)
# mydb.commit()
# print(mycursor.rowcount, "record(s) affected")

######### DELETE ##############
sql = "DELETE FROM demo WHERE customerId = 200"
mycursor.execute(sql)
mydb.commit()
print(mycursor.rowcount, "record(s) deleted")




