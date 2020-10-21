'''
This is a utility/helper program used along with Emarsys Email Marketing platform API.  
It is used to create audience lists and segments programmatically.

For more info on Emarsys Marketing Auire APIs, please refer the following URL
List of all public API endpoints
https://help.emarsys.com/hc/en-us/articles/115005253125-List-of-all-public-API-endpoints

'''

import csv, http.client, json
from emarsysheader import getAuthHeader  # This library is for authenticating user in Emarsys platform.

def checkEmailIDs(contacts,accountName):
	# Get the segment id for a given segment name
	api_path = '/api/v2/contact/checkids'
	payload="{\"key_id\":\"3\",\"external_ids\":" + json.dumps([contact for contact in contacts]) + "}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("POST", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	jdata = json.loads(data)
	conn.close()
	return jdata


def getSegmentId(segmentName,accountName):
	# Get the segment id for a given segment name
	api_path = '/api/v2/filter'
	payload = "{}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("GET", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	jdata = json.loads(data)
	conn.close()
	return next((item["id"] for item in jdata["data"] if item["name"] == segmentName), None)


def deleteSegment(segmentId,accountName):
	# Delete the given segment 
	api_path = '/api/v2/filter/'+str(segmentId)+'/delete'
	payload = "{}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("GET", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	# jdata = json.loads(data)
	conn.close()
	# return json.loads(data)["replyText"]
	return data


def getContactListId(contactListName,accountName):
	# Get the ContactList id for a given contact list
	api_path = '/api/v2/contactlist'
	payload = "{}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("GET", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	jdata = json.loads(data)
	conn.close()
	return next((item["id"] for item in jdata["data"] if item["name"] == contactListName), None)

def getContactsFromList(contactListName,accountName):
	# Get the ContactList id for a given contact list
	contactListID = getContactListId(contactListName,accountName)
	api_path = '/api/v2/contactlist/' + contactListID 
	payload = "{}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("GET", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	jdata = json.loads(data)
	conn.close()
	return jdata['data']
	
def getEmailsFromList(contactListName,accountName):
	# Get the ContactList id for a given contact list
	contactListID = getContactListId(contactListName,accountName)
	listContacts = getContactsFromList(contactListName,accountName)
	offset = 0
	limit = 500
	setSuperEmails = set()
	while offset < len(listContacts):
		api_path = '/api/v2/contactlist/' + contactListID + '/contacts/data?limit='+str(limit)+'&offset='+str(offset)+'&fields=3'
		payload = "{}"
		headers = getAuthHeader(accountName)
		conn = http.client.HTTPSConnection("api.emarsys.net")
		conn.request("GET", api_path, payload, headers)
		res = conn.getresponse()
		data = res.read()
		jdata = json.loads(data)
		for _, data in jdata['data'].items():
			setSuperEmails.add(data['fields']['3'])
		
		offset = offset + limit
	conn.close()
	return setSuperEmails

def deleteContactList(contactListID,accountName):
	# Delete the given segment 
	api_path = '/api/v2/contactlist/'+str(contactListID)+'/deletelist'
	payload = "{}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("POST", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	conn.close()
	return data


def getContactsFromFile(fileName):
	with open(fileName) as f:
	   reader=csv.DictReader(f)
	   return list(reader)
	   
# Payload for creating contacts from CSV file

def createContactsFromFile(fileName,accountName):
	# Creates contacts from the file
	api_path = '/api/v2/contact/?create_if_not_exists=1'
	jContacts = json.dumps(getContactsFromFile(fileName))
	payload="{\"key_id\":\"3\",\"contacts\":" + jContacts + "}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("PUT", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	conn.close()
	return data
	
def createContactsFromDataFrame_v0(dataFrame,accountName):
	# Creates contacts from the file
	api_path = '/api/v2/contact/?create_if_not_exists=1'
	jContacts = dataFrame.to_json(orient='records')
	payload="{\"key_id\":\"3\",\"contacts\":" + jContacts + "}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("PUT", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	conn.close()
	return data

def createContactsFromDataFrame(dataFrame,accountName):
	# Creates contacts from a dataframe
	api_path = '/api/v2/contact/?create_if_not_exists=1'
	offset=0
	data=[]
	while offset < len(dataFrame):
		jContacts = dataFrame[offset:offset+1000].to_json(orient='records')
		offset = offset + 1000
		payload="{\"key_id\":\"3\",\"contacts\":" + jContacts + "}"
		headers = getAuthHeader(accountName)
		conn = http.client.HTTPSConnection("api.emarsys.net")
		conn.request("PUT", api_path, payload, headers)
		res = conn.getresponse()
		data.append(res.read())
		conn.close()
	return data

def createContactsList(listName, fileName, accountName):
	# Creates contacts from the file
	api_path = '/api/v2/contactlist'
	contacts=getContactsFromFile(fileName)
	payload = "{\"key_id\":\"3\",\"name\":\"" + listName + "\","
	payload = payload + "\"external_ids\":" + json.dumps([contact["3"] for contact in contacts]) + "}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("POST", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	conn.close()
	return data
	

def createContactsListFromDataFrame(listName, dataFrame, accountName):
	# Creates contacts from the file
	api_path = '/api/v2/contactlist'
	contacts = list(dataFrame['3'])
	payload = "{\"key_id\":\"3\",\"name\":\"" + listName + "\","
	payload = payload + "\"external_ids\":" + json.dumps([contact for contact in contacts]) + "}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("POST", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	conn.close()
	return data

def overwriteContactListFromDataFrame(listName, dataFrame, accountName):
	# Overwrite the contactList 
	contactListID = getContactListId(listName, accountName)
	api_path = '/api/v2/contactlist/'+str(contactListID)+'/replace'
	contacts = list(dataFrame['3'])
	payload = "{\"key_id\":\"3\",\"name\":\"" + listName + "\","
	payload = payload + "\"external_ids\":" + json.dumps([contact for contact in contacts]) + "}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("POST", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	conn.close()
	return data

def addContactsListFromDataFrame(listName, dataFrame, accountName):   #Contacts - HBO Purchasers - Consolidated
	# Creates contacts from the file
	contactListID = getContactListId(listName,accountName)
	if contactListID == None:
		data = createContactsListFromDataFrame(listName, dataFrame, accountName)
		return data
	api_path = '/api/v2/contactlist/'+str(contactListID)+'/add'
	contacts = list(dataFrame['3'])    #Assuming the dataframe has column 3 which is a field_id for email.
	payload = "{\"key_id\":\"3\",\"name\":\"" + listName + "\","
	payload = payload + "\"external_ids\":" + json.dumps([contact for contact in contacts]) + "}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("POST", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	conn.close()
	return data

def createSegment_v0(segmentName, contactListID, accountName):
	# Creates contacts from the file
	api_path = '/api/v2/filter'
	payload = "{\"name\":\"" + segmentName + "\", \"baseContactListId\":\"" + str(contactListID) + "\"}"
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("PUT", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	conn.close()
	return data

def createSegment(segmentName, contactListID, accountName):
	# Creates contacts from the file
	api_path = '/api/v2/filter'
	payload = "{\"name\":\"" + segmentName + "\", \"baseContactListId\":\"" + str(contactListID) + "\","
	payload = payload + """
	  "contactCriteria": {
		"type": "and",
		"children": [
		  {
			"type": "criteria",
			"field": "optin",
			"operator": "equals",
			"value": "true"
		  }
		]
	  }
	}
	"""
	headers = getAuthHeader(accountName)
	conn = http.client.HTTPSConnection("api.emarsys.net")
	conn.request("PUT", api_path, payload, headers)
	res = conn.getresponse()
	data = res.read()
	conn.close()
	return data



def main(): pass

if __name__ == "__main__": main()


