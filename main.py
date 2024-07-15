
import requests
import hmac
import hashlib
import base64
from datetime import datetime
import urllib
import json


def getCosmosAuthStr(verb, resourceType,resourceId,key, dttm ):
    
    payload = verb + '\n' + resourceType + '\n' + resourceId + '\n' + dttm + '\n\n'
    payload = payload.lower()
    print(payload)
    payload = bytes(payload,encoding='utf-8')
    key = base64.b64decode(key.encode('utf-8'))
    signature = base64.b64encode(hmac.new(key, msg = payload, digestmod = hashlib.sha256).digest()).decode()
    print (signature)
    authStr = urllib.parse.quote('type=master&ver=1.0&sig={}'.format(signature))
    print (authStr)
    return authStr

#List Collections
def  getCollections(dburl, resourceid,key,now):
    authStr = getCosmosAuthStr("GET","colls",resourceid,key,now)
    headers = {
        'Authorization': authStr,
        "x-ms-date": now,
        "x-ms-version": "2017-02-22"
    }
    print ( headers)
    res = requests.get(dburl, headers = headers)
    print (res.text)


#Create Collections
def  createCollection(dburl, collname, ru, resourceid,key,now):
    authStr = getCosmosAuthStr("POST","colls",resourceid,key,now)
    headers = {
        'Authorization': authStr,
        "x-ms-date": now,
        "x-ms-version": "2017-02-22",
        "x-ms-offer-throughput": ru,

    }

    body = {  
        "id": collname,
          "indexingPolicy": {  
            "automatic": True,  
            "indexingMode": "Consistent",  
            "includedPaths": [  
            {  
                "path": "/*",  
                "indexes": [  
                {  
                    "dataType": "String",  
                    "precision": -1,  
                    "kind": "Range"  
                }  
                ]  
            }  
            ]  
        },    
        "partitionKey": {  
            "paths": [  "/AccountNumber"  ],  
            "kind": "Hash",
            "Version": 2
        }  
    }
    print ( headers)
    print ( body)
    res = requests.post(dburl,headers=headers, data=json.dumps(body))
    print (res)
    print (res.text)

key = '<ACCOUNT KEY>'
now = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:00 GMT')
dburl = 'https://<DBACCTNAME>.documents.azure.com/dbs/<DBNAME>/colls'
resourceid = 'dbs/<DBNAME>'

#getCollections(dburl,resourceid, key, now)
createCollection(dburl,'first',"400",resourceid,key, now)