import requests, pymongo, time, json, datetime, os
from requests.auth import HTTPDigestAuth
from datetime import datetime
from dotenv import load_dotenv


"""
#########################################
- Creates an Atlas Cluster
- Loads the sample dataset into that cluster
- Runs the script from the beginning of the lab to fix dates
- Creates an online archive
- Connects to just the cluster to count un-archived documents
- Connects to just the online archive to count archived documents
#########################################
"""

#
# Get Credentials from .env
#

load_dotenv()

digest = HTTPDigestAuth(os.environ.get('apiPub'), os.environ.get('apiPriv'))
driverCreds = os.environ.get('driverCreds')
groupId = os.environ.get('groupId')
connectionStringSubdomain = os.environ.get('connectionStringSubdomain')

#
# Spin Up New Cluster
#

newClusterURL = "https://cloud.mongodb.com/api/atlas/v1.0/groups/"+groupId+"/clusters"

clustername = ("edu-"+str(time.time())).replace(".","")

payload = {
    "autoScaling": {"diskGBEnabled": True},
    "backupEnabled": False,
    "name": clustername,
    "providerSettings": {
        "providerName": "AWS",
        "instanceSizeName": "M30",
        "regionName": "US_EAST_1"
    }
}

response = requests.request("POST", newClusterURL, json=payload, headers={"Content-Type": "application/json"}, params={"pretty":"true"}, auth=digest)

print("New Cluster: "+clustername)

#
# Wait for cluster to load before proceeding
#

mongoURL = "mongodb+srv://"+driverCreds+"@"+clustername+"."+connectionStringSubdomain+".mongodb.net/?retryWrites=true&w=majority"

running = False
tries = 1

# Keep trying to connect until it's finally successful
while running == False:
    try:
        client = pymongo.MongoClient(mongoURL)
        db = client.test
        running = True
    except Exception as e:
        print(str(tries) + ".) waiting on cluster to load...")
        time.sleep(30)
        tries += 1
        pass  # Could happen in face of bad user input 

#
# Insert Data Set
#

print("Inserting documents")

db = client.education
collection = db.student_grades

with open('student_grades.json') as file:
    file_data = json.load(file)

student_grades=[]

# convert dates to datetime

for doc in file_data:

    doc["date_assigned"]=datetime.strptime((doc["date_assigned"]["$date"]).split(".")[0].split("+",1)[0],'%Y-%m-%dT%H:%M:%S')

    if "date_completed" in doc.keys():
        doc["date_completed"]=datetime.strptime((doc["date_completed"]["$date"]).split(".",1)[0].split("+",1)[0],'%Y-%m-%dT%H:%M:%S')
    
    student_grades.append(doc)

collection.drop()

collection = db.student_grades 

if isinstance(student_grades, list):
    collection.insert_many(student_grades) 
else:
    collection.insert_one(student_grades)

#
# Fix Dates
#

print("Fix Dates")

# Find most recent date_completed
maxdate = list(collection.aggregate([
               {
                   "$sort":{"date_completed":-1}
               },
               {
                   "$limit":1
               },
               {
                   "$project":{
                       "max_date_completed":"$date_completed",
                       "_id":0
                   }
               }
           ]))[0]["max_date_completed"]

# Update all date_completed to be closer to today
pipeline = [
   {
       "$addFields":{
           "diff":{
               "$dateDiff":{
                   "startDate": maxdate,
                   "endDate": "$$NOW",
                   "unit": "day"
               }
           }
       }
   },
   {
       "$set":{
           "date_assigned":{
               "$dateAdd":{
                   "startDate":"$date_assigned",
                   "unit":"day",
                   "amount":{"$subtract":["$diff",1]}
               }
           },
           "date_completed":{
               "$cond":{
                   "if":{"$eq":["$status","complete"]},
                   "then":{
                       "$dateAdd":{
                           "startDate":"$date_completed",
                           "unit":"day",
                           "amount":{"$subtract":["$diff",1]}
                       }
                   },
                   "else": None
               }
              
           }
       }
   },
   {
       "$unset":"diff"
   }
]

response = collection.update_many(filter = {}, update = pipeline)

print("... Updated")

#
# Create indexes
#

print("Creating indexes")

collection.create_index('date_completed', unique = False)


#
# Count Un-Archived Docs Before Creating Archive
#

before = collection.count_documents({})
print("Before: "+str(before))


#
# Create Online Archive
#

print ("Creating Archive")

createArchiveURL = newClusterURL+"/"+clustername+"/onlineArchives"

archiveConfig = {
        "dbName": "education",
        "collName": "student_grades",
        "partitionFields": [
                {
                        "fieldName": "student_name.last",
                        "order": 0
                },
                {
                        "fieldName": "assignment_name",
                        "order": 1
              }],
        "criteria": {
                "type": "DATE",
                "dateField": "date_completed",
                "dateFormat": "ISODATE",
                "expireAfterDays": 7
  }
}

response = requests.request("POST", createArchiveURL, json=archiveConfig, headers={"Content-Type": "application/json"}, params={"pretty":"true"}, auth=digest)
resp = json.loads(response.text)

archiveID = resp["_id"]
archiveStatus =  resp["state"]
lastArchiveRunEnd = False

#
# Count Un-Archived Docs
#

tries = 1

count = collection.count_documents({})

print("... ID: "+str(archiveID))

# Keep checking API for the archive until lastArchiveRun shows up, indicating it completed the first archive

while lastArchiveRunEnd == False:
    try:
        response = requests.request("GET", createArchiveURL+"/"+str(archiveID), headers={"Content-Type": "application/json"}, params={"pretty":"true"}, auth=digest)
        resp = json.loads(response.text)
        archiveStatus = resp["state"]

        if "lastArchiveRun" in resp.keys():
            lastArchiveRunEnd = True
        else:
            lastArchiveRunEnd = False

        print(str(tries) + ".) "+archiveStatus+" / "+str(count)+" in collection")
    except Exception as e:
        print(str(tries) + ".) missed")
        pass  # Could happen in face of bad user input 
    
    tries += 1
    time.sleep(30)
    count = collection.count_documents({})

print ("... Done")

# How many docs are in the collection now?
after = collection.count_documents({})
print("After: "+str(after))

#
# Count Archived Docs
#

getClusterURL = "https://cloud.mongodb.com/api/atlas/v1.0/groups/"+groupId+"/clusters/"+clustername

response = requests.request("GET", getClusterURL, headers={"Content-Type": "application/json"}, params={"pretty":"true"}, auth=digest)

archiveConnectionString = json.loads(response.text)["connectionStrings"]["onlineArchive"].replace("//","//"+driverCreds+"@archived-")
archiveClient = pymongo.MongoClient(archiveConnectionString)

archiveDB = archiveClient.education
archiveCollection = archiveDB.student_grades

# How many docs are in the archive?
archiveAfter = archiveCollection.count_documents({})
print("Archived: "+str(archiveAfter))