import pymysql
import json
import csv
import os, sys
import json

def RDB_TOP():
	cnx = pymysql.connect(host='localhost',
	                      user='dbuser',
	                      password='dbuser',
	                      db='lahman2017',
	                      charset='utf8mb4',
	                      cursorclass=pymysql.cursors.DictCursor)
	cursor = cnx.cursor()

	q = "SELECT Batting.playerID as playerID, \
		(SELECT People.nameFirst FROM People WHERE People.playerID = Batting.playerID) AS firstname, \
		(SELECT People.nameLast FROM People WHERE People.playerID = Batting.playerID) AS lastname, \
		sum(Batting.AB) AS total_bats, \
		sum(Batting.H) AS total_hits, \
		sum(Batting.H) / sum(Batting.AB) as batting_average, \
		min(Batting.yearID) as first_year, \
		max(Batting.yearID) as last_year \
		FROM Batting \
		GROUP BY playerID \
		HAVING last_year >= 1960 AND total_bats > 200\
		ORDER BY batting_average DESC\
		LIMIT 10;"

	cursor.execute(q)
	r = cursor.fetchall()
	print(json.dumps(r, indent=3))


def CSV_TOP():
	batting_path = os.path.realpath('../Data/Batting.csv')
	people_path = os.path.realpath('../Data/People.csv')

	with open(batting_path, newline='') as file:
	    reader = csv.reader(file)
	    batting_table = []
	    for row in reader:
	        batting_table.append(row)

	with open(people_path, newline='') as file:
	    reader = csv.reader(file)
	    people_table = []
	    for row in reader:
	        people_table.append(row)

	dic = {}
	playerID_index, AB_index, H_index, year_index = batting_table[0].index("playerID"), batting_table[0].index("AB"), batting_table[0].index("H"), batting_table[0].index("yearID")
	for row in batting_table[1:]:
		if row[playerID_index] not in dic:
			dic[row[playerID_index]] = [{"AB":row[AB_index], "H":row[H_index], "yearID":row[year_index]}]
		else:
			dic[row[playerID_index]].append({"AB":row[AB_index], "H":row[H_index], "yearID":row[year_index]})

	backup = {}
	for p in dic:
		for elem in dic[p]:
			if p in backup:
				backup[p]["total_AB"] += int(elem["AB"])
				backup[p]["total_H"] += int(elem["H"])
				backup[p]["year"] = max(int(elem["yearID"]), backup[p]["year"])
			else:
				backup[p] = {"playerID":p, "total_AB":int(elem["AB"]), "total_H":int(elem["H"]), "year":int(elem["yearID"])}

	for p in backup:
		backup[p]["average"] = backup[p]["total_H"] / backup[p]["total_AB"] if backup[p]["total_AB"] != 0 else 0
		if backup[p]["year"] >= 1960 and backup[p]["total_AB"] > 200:
			for row in people_table[1:]:
				if row[people_table[0].index("playerID")] == p:
					backup[p]["nameFirst"] = row[people_table[0].index("nameFirst")] 
					backup[p]["nameLast"] = row[people_table[0].index("nameLast")]
					
	res = []
	for p in backup:
		if backup[p]["year"] >= 1960 and backup[p]["total_AB"] > 200:
			res.append(backup[p])
			res.sort(key = lambda elem:elem["average"])
			if len(res) > 11:
				res.pop(0)
	res = res[:-1][::-1]
	print("Top 10 Hitters: ")
	print(json.dumps(res, indent=3))
