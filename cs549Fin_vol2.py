import sys
import pandas as pd
import pymysql
import self as self
import random

print("BUBA")



SortByReward = 0



def results():
    db = pymysql.connect('localhost','root','root','2019bollywood')
    cursor = db.cursor()
    cursor.execute("SELECT * FROM mytable WHERE Movie_type LIKE '%Drama%'")
    dbresponse = cursor.fetchall()
    #for row in dbresponse:
        #print(row[2])
        #if row[2] == 10.0:
            #SortByReward = 1


    if SortByReward == 0:
        cursor.execute("SELECT * FROM mytable WHERE Movie_type LIKE '%Drama%' ORDER BY Crtitic_ratings")
        dbresponse = cursor.fetchall()
        FloatRateSum = 0.0
        for row in dbresponse:
            FloatRating = float(row[2] or 0)
            #print(IntRating)
            FloatRateSum = FloatRateSum + FloatRating

        for row in dbresponse:
            FloatRatingNew = float(row[2] or 0)
            ProbDisplay = FloatRatingNew / FloatRateSum
            if random.random() <= ProbDisplay:
                  print(row)

    winresult = input("Please select a good tuple result: ")

    for row in dbresponse:
        StrTitle = str(row[0] or "None")
        FloatRatingNewDecayed = float(row[2] or 0)
        FloatDece = FloatRatingNewDecayed / 10
        if StrTitle == winresult:
            sql = " UPDATE mytable SET Crtitic_ratings = %s WHERE Movie_name = %s"
            cursor.execute(sql, (FloatDece, StrTitle))
            db.commit()



    for row in dbresponse:
        print(row)
    #if row[0] == winresult:
       # db = pymysql.connect('localhost', 'root', 'root', '2019bollywood')
        #cursor = db.cursor()
        #sql = "UPDATE mytable SET Rating = 6 WHERE Movie_type LIKE 'Drama'"
        #cursor.execute(sql)
        #db.commit()

    #print("{0} {1}".format(row[0], row[1]))

results()
