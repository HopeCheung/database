import os, sys
sys.path.append(sys.path[0][:-5]+"/src")
import CSVDataTable

#load 
bat_obj = CSVDataTable.CSVDataTable("Batting", ["playerID","yearID","stint","teamID"])
bat_obj.load()

peo_obj = CSVDataTable.CSVDataTable("People", ["playerID"])
peo_obj.load()
print("Test output format for People.csv:")
print(peo_obj)
print("\n-----------------------------\n")

print("Test find_by_primary_key with primary keys for Batting: ['beckero01','1996','1','COL']:")
print(bat_obj.find_by_primary_key(["beckero01","1996","1","COL"], ["playerID", "teamID", "CS","SB"]))
print("Test find_by_primary_key with primary keys for Batting with no result:")
print(bat_obj.find_by_primary_key(["hope","2096","1","COL"], ["playerID", "teamID", "CS","SB"]))
print("\n-----------------------------\n")

print("Test find_by_template with template for People: {'birthYear': '1988', 'birthMonth':'5', 'birthDay':'20'}:")
test_instance = peo_obj.find_by_template({'birthYear': '1988','birthMonth':'5', 'birthDay':'20'}, ["playerID", "birthYear", "birthMonth", "birthDay"])
print(test_instance)
print("Derived table content:")
print(test_instance.table_content)
print("Test find_by_template function in DerivedTable:")
print(test_instance.find_by_template({"playerID":"riverca02"}).table_content)
print("\n-----------------------------\n")


print("Test insert:'playerID':'Hope', 'yearID':'1997', 'stint':'1', 'teamID':'Columbia':")
bat_obj.insert({"playerID":"Hope", "yearID":"1997", "stint":"1", "teamID":"Columbia",
             "lgID":"NN", "G":"9", "AB":"8", "R":"7","H":"6", "2B":"5", "3B":"4",
             "HR":"3", "RBI":"2", "SB":"1","CS":"0", "BB":"0", "SO":"0", "IBB":"0",
             "HBP":"0", "SH":"0", "SF":"0", "GIDP":"0"})
print("Insertion result:")
print(bat_obj.find_by_primary_key(["Hope", "1997", "1", "Columbia"]))
print("\n-----------------------------\n")

print("Test delete_by_template:")
bat_obj.insert({"playerID":"Hope2", "yearID":"1997", "stint":"1", "teamID":"Columbia",
             "lgID":"NN", "G":"9", "AB":"8", "R":"7","H":"6", "2B":"5", "3B":"4",
             "HR":"3", "RBI":"2", "SB":"1","CS":"0", "BB":"0", "SO":"0", "IBB":"0",
             "HBP":"0", "SH":"0", "SF":"0", "GIDP":"0"})
print("Before deletion, the target line is shown as:")
print(bat_obj.find_by_template({"teamID":"Columbia"}).table_content)
r = bat_obj.delete_by_template({"teamID":"Columbia", "yearID":"1997"})
print("After deletion, the target line is shown as:")
print(bat_obj.find_by_template({"teamID":"Columbia"}).table_content)
print("Delete counting number:{}".format(r))
print("\n-----------------------------\n")

print("Test delete_by_primary_key:")
bat_obj.insert({"playerID":"Hope", "yearID":"1997", "stint":"1", "teamID":"Columbia",
             "lgID":"NN", "G":"9", "AB":"8", "R":"7","H":"6", "2B":"5", "3B":"4",
             "HR":"3", "RBI":"2", "SB":"1","CS":"0", "BB":"0", "SO":"0", "IBB":"0",
             "HBP":"0", "SH":"0", "SF":"0", "GIDP":"0"})
print("Before deletion, the target line is shown as:")
print(bat_obj.find_by_primary_key(["Hope", "1997", "1", "Columbia"]))
r = bat_obj.delete_by_key(["Hope", "1997", "1", "Columbia"])
print("After deletion, the target line is shown as:")
print(bat_obj.find_by_primary_key(["Hope", "1997", "1", "Columbia"]))
print("Delete counting number:{}".format(r))
print("\n-----------------------------\n")

print("Test update_by_template:")
bat_obj.insert({"playerID":"Hope", "yearID":"1997", "stint":"1", "teamID":"Columbia",
             "lgID":"NN", "G":"9", "AB":"8", "R":"7","H":"6", "2B":"5", "3B":"4",
             "HR":"3", "RBI":"2", "SB":"1","CS":"0", "BB":"0", "SO":"0", "IBB":"0",
             "HBP":"0", "SH":"0", "SF":"0", "GIDP":"0"})
bat_obj.insert({"playerID":"Hope2", "yearID":"1997", "stint":"1", "teamID":"Columbia",
             "lgID":"NN", "G":"9", "AB":"8", "R":"7","H":"6", "2B":"5", "3B":"4",
             "HR":"3", "RBI":"2", "SB":"1","CS":"0", "BB":"0", "SO":"0", "IBB":"0",
             "HBP":"0", "SH":"0", "SF":"0", "GIDP":"0"})
print("Before update, the target line is shown as:")
print(bat_obj.find_by_template({"teamID":"Columbia"}).table_content)
print("After update stint to 2, the target line is shown as:")
r = bat_obj.update_by_template({"teamID":"Columbia"}, {"stint":"2"})
print(bat_obj.find_by_template({"teamID":"Columbia"}).table_content)
print("Update counting number:{}".format(r))
print("\n-----------------------------\n")


print("Test update_by_key:")
print("Before update, the target line is shown as:")
print(bat_obj.find_by_primary_key(["Hope", "1997", "2", "Columbia"]))
print("After update stint to 3, the target line is shown as:")
r = bat_obj.update_by_key(["Hope", "1997", "2", "Columbia"], {"stint":"3"})
print(bat_obj.find_by_primary_key(["Hope", "1997", "3", "Columbia"]))
print("Update counting number:{}".format(r))
print("Update Duplicate Errors:")
print(bat_obj.find_by_template({"teamID":"Columbia"}).table_content)
bat_obj.update_by_key(["Hope2", "1997", "2", "Columbia"],{"playerID":"Hope", "stint":"3"})
print("\n-----------------------------\n")



