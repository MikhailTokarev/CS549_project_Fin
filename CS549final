import sys
import pandas as pd
import pymysql
import self as self

print("BUBA")







def results():
    db = pymysql.connect('localhost','root','root','fullbollywood')
    cursor = db.cursor()
    cursor.execute("SELECT * FROM mytable WHERE Tittle LIKE 'War' GROUP BY Rating")
    dbresponse = cursor.fetchall()
    for row in dbresponse:
        print("{0} {1}".format(row[0], row[1]))

    winresult = input("Please select a good tuple result: ")
    if row[0] == winresult :

        db = pymysql.connect('localhost', 'root', 'root', 'fullbollywood')
        cursor = db.cursor()
        sql = "UPDATE mytable SET Rating = 6 WHERE Tittle = 'War'"
        cursor.execute(sql)
        db.commit()

    for row in dbresponse:
        print("{0} {1}".format(row[0], row[1]))



results()
