import sys
import pandas as pd
import pymysql
import self as self
import random
import sys
import pandas as pd
import pymysql
import self as self
import random
import wikipedia
import csv
import wikipediaapi
from datetime import datetime
from datetime import date
import Levenshtein
import itertools


#print("BUBA")

#username = input("Please enetr your user name: ")
username = "Grisha"
username = str(username)
db = pymysql.connect('localhost', 'root', 'root', '2019bollywood')
#SortByReward = 0



#UserInputCountry = input("What Country Production You prefer? (enter to skip): ")
UserInputCountry = "Russia"
#UserInputCast = input("What Cast You prefer? (enter to skip): ")
UserInputCast = "MrWhite"
#UserInputDirector = input("Any Specific Director? (enter to skip): ")
UserInputDirector = "MrPink"
#UserInputGenre = input("What Genre You prefer? (enter to skip): ")
UserInputGenre = "%Thriller%"

#class Genre:
#    def __init__(self,genre):
#        self.genre = genre
#
#
#    def pick_genre(self):
#        print('Your genre is' , self)



def diff_dates(date1, date2):
    return abs(date2-date1).days


def JustPrint():

    cursor = db.cursor()
    UserInput = "Drama"
    sql1 = "SELECT * FROM globalfilms WHERE Genre LIKE %s ORDER BY Rating"
    cursor.execute(sql1, UserInput)
    dbresponse = cursor.fetchall()
    for row in dbresponse:
        print(row)


# print(row[8])

def results():
    resultlist = []
    today = date.today()
    cursor = db.cursor()

    sqlwho = "SELECT * FROM userstrategy WHERE username = %s"
    cursor.execute(sqlwho,username)
    userstrategy = cursor.fetchall()


    for row in userstrategy:
        userexploration = row[1]
        userexploitation = row[2]


    if UserInputGenre:
        sql1 = "SELECT * FROM globalfilms WHERE Genre LIKE %s ORDER BY Rating"
        cursor.execute(sql1, UserInputGenre)
        dbresponse = cursor.fetchall()

        sql2 = "SELECT * FROM usermovielist WHERE username LIKE %s"
        cursor.execute(sql2,username)
        userdbresponse = cursor.fetchall()

    FloatRateSum = 0.0
    FloatRating = 0.0
    similarity = 1
    numberofresults = 0
    countresults = 0
    strnumber = "hui"
    numberofuserspicks = 0
    NewFloatRate = 0
    ProbToBeShow = 0
    # userexploitation = 1
    # userexploration = 1
    for row in dbresponse:
        numberofresults += 1
        if row[8]:
            FloatRating = float(row[8] or 0)
        FloatRateSum = FloatRateSum + FloatRating
        strnumber = str(numberofresults)

    print("The number of possible results is :" + strnumber)
    print("Float rate sum is: " + str(FloatRateSum))

    for row in dbresponse:
        for userrow in userdbresponse:
            numberofuserspicks += 1
            strgenre = str(row[3])
            strcast = str(row[4])
            strcountry = str(row[5])
            userstrgenre = str(userrow[4])
            levendiffernecegenre = Levenshtein.distance(strgenre, userstrgenre)
            if UserInputCast in strcast:
                similarity += 10
            if levendiffernecegenre == 0:
                similarity += 10
            if UserInputGenre in strgenre:
                similarity += 5
            if  UserInputCountry in strcountry:
                similarity += 10
    #struserpicks = str(numberofuserpicks)
        NewFloatRate = round(random.uniform(0.0,10.0),1)
        ProbToBeShow =   NewFloatRate * similarity * userexploration / userexploitation
        print("Probabiluity to be shown : " + str(ProbToBeShow) + " Where Main rank is : " + str(NewFloatRate) + " Where exploit/explore is " + str(userexploration) + "/" + str(userexploitation))
        vandabuba = str(levendiffernecegenre)
        print("levendifference :" + vandabuba)

        if random.uniform(0.0,100000000) < ProbToBeShow:
            print(row)
            resultlist.append(row)
            countresults += 1
    print("Number of results before sampling :" + str(countresults))


    k = 10
    reservoir = [0] * k
    for i in range(k):
        reservoir[i] = resultlist[i]

    while(i < countresults - 1):
        j = random.randrange(i + 1)
        if j < k:
            reservoir[j] = resultlist[i]
        i += 1

    for unit in reservoir:
        print(unit)
    #print(reservoir,k)




def pick():
    today = date.today()
    winresult = input("Please select a good tuple result: ")
    cursor = db.cursor()
    cursor.execute("SELECT * FROM globalfilms")
    dbresponse = cursor.fetchall()
    cursor.execute("SELECT * FROM usermovielist")
    dbuserresponse = cursor.fetchall()
    countuserlist = 0
    # for row in dbresponse:
    #     StrTitle = str(row[1] or "None")
    #     if StrTitle == winresult:
    #         UpdateUserDB(username,row[1],row[5],row[6],row[3],today)
    #         today = date.today()
    #         sql2 = "UPDATE usermovielsit SET datewatched = %s WHERE title = %s"
    #         cursor.execute(sql2,(today,StrTitle))
    #         db.commit()
    #ExploitUpgraded = False
    exploitationincrease = 0
    explrationincrease = 2
    for row in dbresponse:
        StrTitle = str(row[1] or "None")
        StrYear = str(row[2] or "None")
        StrGenre = str(row[3] or "None")
        StrDirector = str(row[4] or "None")
        StrCountry = str(row[5] or "None")
        StrCats = str(row[6] or "None")
        StrCats = StrCats[:250]
        StrDescription = str(row[7] or "None")



        if winresult == StrTitle:
            WinYear = StrYear
            WinGenre = StrGenre
            WinDirector = StrDirector
            WInCountry = StrCountry
            WinCast = StrCats
            WinDescription = StrDescription
            UpdateUserDB(username,StrTitle,StrCountry,StrCats,StrGenre,today)
            for userrow in dbuserresponse:
                countuserlist += 1
                UserStrTitle = str(row[1] or "None")
                UserStrGenre = str(row[4] or "None")
                #UserStrDirector = str(row[4] or "None")
                UserStrCountry = str(row[2] or "None")
                UserStrCats = str(row[3] or "None")
                #UserStrDescription = str(row[7] or "None")

                if StrGenre == UserStrGenre:
                    exploitationincrease += 2
                    print("Exploit increased by 2")
                if StrCountry == UserStrCountry:
                    exploitationincrease += 2
                    print("Exploit increased by 2")
                if StrGenre in UserStrGenre or UserStrGenre in StrGenre:
                    exploitationincrease += 1
                    print("Exploit increased by 1")
                if StrCountry in UserStrCountry or UserStrCountry in StrCountry:
                    exploitationincrease += 1
                    print("Exploit increased by 1")
    explrationincrease = countuserlist / 2
    UpgradeExploit(username,exploitationincrease)
    UpgradeExplroe(username,explrationincrease)

    # for userrow in dbuserresponse:
    #     UserStrTitle = str(userrow[1] or "None")
    #     UserStrGenre = str(userrow[4] or "None")
    #
    #
    #     if UserStrTitle == winresult:
    #         UpgradeExploit(username)
    #         ExploitUpgraded = True
    #         print("Exploitation strategy of the user increased")
    #
    #
    # if ExploitUpgraded == False:
    #     UpgradeExploit(username)
    #     print("Exploration strategy of the user increased")
    #for row in dbresponse:
        #print(row)


def CreateUserDB():
    #
    UserName = input("Please enter your name: ")
    cursor = db.cursor()
    #cursor.execute("CREATE TABLE usermovielist (username VARCHAR(255), title VARCHAR(255), country VARCHAR(255),cast VARCHAR(255), genre VARCHAR(255),   datewatched datetime)")
    cursor.execute("CREATE TABLE userstrategy (username VARCHAR(255), exploration INT(10), exploitation INT(10))")

def UpdateUserDB(username,title,country,cast,genre,datewatch):
    cursor = db.cursor()
    sql = "INSERT INTO usermovielist(username,title,country,cast,genre,datewatched) VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql,(username,title,country,cast,genre,datewatch))
    db.commit()

def UpdateStrategyDB(username):
    expo = 1
    explo = 1
    cursor = db.cursor()
    sql = "INSERT INTO userstrategy(username,exploration,exploitation) VALUES (%s,%s,%s)"
    cursor.execute(sql,(username,expo,explo))
    db.commit()

def UpgradeExplroe(username,number):
    cursor = db.cursor()
    sql2 = "SELECT * FROM userstrategy WHERE username LIKE %s"
    cursor.execute(sql2,username)
    dbresponse = cursor.fetchall()
    for row in dbresponse:
        NewExplore = row[2] + number
        sql2 = "UPDATE userstrategy SET exploration = %s WHERE username = %s"
        cursor.execute(sql2,(NewExplore,username))
    db.commit()



def UpgradeExploit(username,number):
    cursor = db.cursor()
    sql2 = "SELECT * FROM userstrategy WHERE username LIKE %s"
    cursor.execute(sql2, username)
    dbresponse = cursor.fetchall()
    for row in dbresponse:
        NewExploit = row[1] + number
        sql2 = "UPDATE userstrategy SET exploitation = %s WHERE username = %s"
        cursor.execute(sql2, (NewExploit, username))
    db.commit()



def PrintUserTable():
    cursor = db.cursor()
    cursor.execute("SELECT * FROM usermovielist")
    dbase = cursor.fetchall()
    for row in dbase:
        print(row)


def PrintStrategyTable():
    cursor = db.cursor()
    cursor.execute("SELECT * FROM userstrategy")
    dbase = cursor.fetchall()
    for row in dbase:
        print(row)

#results()

#thedate = date.today()

#CreateUserDB()

#pick()
#UpdateStrategyDB(username)
#UpdateUserDB("Misha","Tony","Russia","Bony and Pony","Drama",thedate)
#PrintUserTable()
#PrintStrategyTable()
#CreateUserDB()
#JustPrint()

inputuser = input("Do you have username? (yes/no)")
if inputuser == "no":
    newuser = input("Please enter your username :")
    UpdateStrategyDB(newuser)

if inputuser == "yes":
    username = input("Please enter your username :")
    while 1 == 1:
        print("Options: (Explore films , Show my list, Show my strategy, ")
        theinput = input("What do you want to do? ")

        if theinput == "Explore films" :
            results()
            pick()
        if theinput == "Show my list" :
            PrintUserTable()
        if theinput == "Show my strategy" :
            PrintStrategyTable()
