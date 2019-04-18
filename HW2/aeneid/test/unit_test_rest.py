# Haopeng Zhang - hz2558
import requests
import json

def test_api_1():
    r = requests.get("http://127.0.0.1:5000")
    result = r.text

    print("First REST API returned.", r.text)

def test_json():
    print("-----------------------Test json1-----------------------")
    params = {"nameLast": "Williams", "fields": "playerID, nameLast, nameFirst"}
    url = 'http://127.0.0.1:5000/api/lahman2017/people'
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    r = requests.get(url, headers=headers, params=params)
    print("Result = ")
    print(r.text)
    print(json.dumps(r.json(), indent=2, default=str))

def test_json2():
    print("-----------------------Test json2-----------------------")
    url = 'http://127.0.0.1:5000/explain/body'
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    data = {"p": "cool"}
    r = requests.post(url, headers=headers, json=data)
    print("Result = ")
    print(json.dumps(r.json(), indent=2, default=str))

def test_get1():
    print("-----------------------Test get /api/<dbname>/<table_name>q=<some_query_string----------------------->")
    url = "http://127.0.0.1:5000/api/lahman2017/people"

    querystring = {"fields": "playerID,nameLast,nameFirst,birthCity", "nameLast": "Williams",
                   "birthCity": "San Diego", "limit": "10"}

    payload = ""
    headers = {
        'cache-control': "no-cache",
        'Postman-Token': "679952ca-33ed-4815-8be8-75d0537ce347"
    }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    print("Result = ")
    print(json.dumps(response.json(), indent=2, default=str))


def test_get2():
    print("-----------------------Test get /api/<dbname>/<table_name>q=<some_query_string----------------------->")
    url = "http://127.0.0.1:5000/api/lahman2017/people"

    querystring = {"children": "appearances,batting", "nameLast": "Williams", "batting.yearID": "1960",
                   "appearances.yearID": "1960",
                   "fields": "playerID,nameLast,nameFirst,batting.AB,batting.H,appearances.G_all,appearances.GS"}

    payload = ""
    headers = {
        'cache-control': "no-cache",
        'Postman-Token': "484a21db-52ac-421f-b30b-353d468219ec"
    }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    print("Result = ")
    print(json.dumps(response.json(), indent=2, default=str))

def test_get3():
    print("-----------------------Test get /api/<dbname>/<table_name>/<primary_key>/<table2_name>?query_string:-----------------------")
    url = "http://127.0.0.1:5000/api/lahman2017/people/willite01/batting"

    querystring = {"fields": "ab,h", "yearid": "1960"}

    payload = ""
    headers = {
        'cache-control': "no-cache",
        'Postman-Token': "9b4cd810-4a6a-47e5-853b-1d5e1cf222e1"
    }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    print("Result = ")
    print(json.dumps(response.json(), indent=2, default=str))

def test_get4():
    print("-----------------------Test get /api/<dbname>/<table_name>q=<some_query_string----------------------->")
    url = "http://127.0.0.1:5000/api/lahman2017/batting"

    querystring = {"children": "teams",
                   "fields": "batting.playerID,batting.teamID,batting.yearID,batting.H,batting.HR,batting.RBI,teams.yearID,teams.teamID,teams.W",
                   "batting.teamID": "BOS", "batting.yearID": "1960"}

    payload = ""
    headers = {
        'cache-control': "no-cache",
        'Postman-Token': "06469ca6-17a5-494b-af03-46c5f098b2ec"
    }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    print("Result = ")
    print(json.dumps(response.json(), indent=2, default=str))

def test_post():
    print("-----------------------Test post /api/<dbname>/<table_name>-----------------------")
    url = "http://127.0.0.1:5000/api/lahman2017/batting"

    payload = "{\n\t\"playerID\": \"abbotfr01\",\n\t\"yearID\":\"1903\",\n\t\"teamID\":\"CLE\",\n\t\"lgID\":\"AL\",\n\t\"stint\": \"3\",\n\t\"G\":\"34\",\n\t\"AB\":\"30\",\n\t\"H\":\"30\"\n}"
    headers = {
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "b448d8dc-9a83-49af-b687-a2b6fe267424"
    }

    response = requests.request("POST", url, data=payload, headers=headers)
    print("status about posting:")
    print(response.text)

def test_delete():
    print("-----------------------Test delete /api/<dbname>/<table_name>/<primary_key>-----------------------")
    url = "http://127.0.0.1:5000/api/lahman2017/batting/abbotfr01_CLE_1903_3"

    payload = ""
    headers = {
        'cache-control': "no-cache",
        'Postman-Token': "1d040551-5eff-4ba2-92bf-f5d8f0c0c682"
    }

    response = requests.request("DELETE", url, data=payload, headers=headers)
    print("status about deleting:")
    print(response.text)

def test_post2():
    print("-----------------------Test post /api/<dbname>/<table_name>/<primary_key>/table_name-----------------------")
    import requests

    url = "http://127.0.0.1:5000/api/lahman2017/people/abbotfr01/batting"

    payload = "{\n\t\"yearID\":\"1903\",\n\t\"teamID\":\"CLE\",\n\t\"lgID\":\"AL\",\n\t\"stint\": \"5\",\n\t\"G\":\"34\",\n\t\"AB\":\"30\",\n\t\"H\":\"30\"\n}"
    headers = {
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "a3ed3a0b-f4f9-41f7-af7f-ff065875e8d6"
    }

    response = requests.request("POST", url, data=payload, headers=headers)

    print("status about posint:")
    print(response.text)


def test_put():
    print("-----------------------Test put /api/<dbname>/<table_name>/<primary_key>-----------------------")
    url = "http://127.0.0.1:5000/api/lahman2017/batting/abbotfr01_CLE_1903_5"

    payload = "{\n\t\"G\": \"32\",\n\t\"AB\":\"120\"\n}"
    headers = {
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "c59b863b-c031-4c46-ae71-1004d27c5bff"
    }

    response = requests.request("PUT", url, data=payload, headers=headers)
    print("status about updating:")
    print(response.text)


# test_api_1()
# test_json() # change _project() in RDBDataTable
test_json2()
test_get1()
test_get2()
test_get3()
test_get4()
test_post()
test_delete()
test_post2()
test_put()
