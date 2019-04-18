from aeneid.dbservices.RDBDataTable import RDBDataTable

def test_create():
    tbl = RDBDataTable("people")
    print("test_create: tbl = ", tbl)

print("test_create()")
test_create()
print("Once more with feeling!")
test_create()