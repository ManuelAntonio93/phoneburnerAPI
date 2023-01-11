# -*- coding: utf-8 -*-
"""
Created on Thu Nov  4 10:12:35 2021

@author: Antonio Noriega
"""

#PREPARING THE ENVIROMENT

import requests
import pandas as pd
import datetime
import time
import numpy as np
from phconfig import settings

##################### AUTHENTICATION PHONEBURNER ##############################

#Data Body Refresh Token

client_id = settings.client_id

client_secret = settings.client_secret

redirect_uri = settings.redirect_uri

refresh_token = settings.refresh_token

grant_type = "refresh_token"


#Request Regresh Token

rtoken_ep = "https://www.phoneburner.com/oauth/refreshtoken"

data = {"client_id":client_id, "client_secret":client_secret, 
        "redirect_uri":refresh_token, "refresh_token":refresh_token, 
        "grant_type":grant_type  }

#Get Access Token
atoken = requests.post(rtoken_ep, data=data).json()
atoken = atoken["access_token"]

################### GET ALL USERS PERSONAL TOKENS ############################


#User manuel@infusion51a.com ID: 624892681
#User jeff@infusion51a.com ID: 	624896739
#User sean@infusion51a.com ID: 736981101

#List users ID
users_id = ["624892681","624896739","736981101"]
user_tokens = []

#Parameters

members_ep = "https://www.phoneburner.com/rest/1/members"
headers = {"Authorization": "Bearer {}".format(atoken)}

#Iteration to get the User Tokens

for i in users_id:
      user_tokens.append(requests.get(members_ep, headers=headers, params={"user_id":i}).json()["members"]["members"][0]["oauth"]["bearer_token"])

##################### GET DIAL SESSIONS FROM ALL USERS #######################


##Editable Date Range

dial_session_ep = "https://www.phoneburner.com/rest/1/dialsession"
date_start = "2021-09-01"
date_end = datetime.datetime.now()
date_end = date_end.strftime("%Y-%m-%d")
payload = {"date_start":date_start, "date_end":date_end}

#Create Dataframe to store all call records
df = pd.DataFrame()

#Define Function to flat lists
def flatten(t):
    return [item for sublist in t for item in sublist]

#Iteration to get Dial Sessions ID for all users
start = time.time()

for token in list(user_tokens.values()):
    
    
    #Create list of dial sessions ids                                           
    
    dial_sessions_ids =[]
    total_pages = requests.get(dial_session_ep, headers = {"Authorization":"Bearer {}".format(token)}, params=payload).json()["dialsessions"]["total_pages"]   
    
    
    for page in range(1,total_pages+1):
        
        rdial_sessions = requests.get(dial_session_ep, headers = {"Authorization":"Bearer {}".format(token)}, 
                                      params = {"date_start":date_start, "date_end":date_end, "page":page}).json()["dialsessions"]["dialsessions"]
        dial_sessions_ids.append([ds_id["dialsession_id"] for ds_id in rdial_sessions])
    
    
    dial_sessions_ids = flatten(dial_sessions_ids)
    
    for dsid in dial_sessions_ids:
        
        ep_ds = "https://www.phoneburner.com/rest/1/dialsession/{}".format(dsid)
        
        total_pages =requests.get(ep_ds, headers = {"Authorization":"Bearer {}".format(token)},
                             params={"page":page}).json()["dialsessions"]["total_pages"]
        
        for page in range(1,total_pages+1):
            
            calls = requests.get(ep_ds, headers = {"Authorization":"Bearer {}".format(token)},
                                 params={"page":page}).json()["dialsessions"]["dialsessions"]["calls"]
        
            for call in calls: 
                call['account_id'] = [k for k, v in user_tokens.items() if v == token][0]
                df = df.append(call, ignore_index=True)
                
end = time.time()   

print(end-start)

#Convert df into excel
df.to_excel('Call_log_PB.xlsx')

######################## GET CALL RECORDINGS #################################

df = pd.read_excel('PhoneBurner_Call_History.xlsx')

#Parameters
callr_ep = "https://www.phoneburner.com/rest/1/dialsession/call"
df_callr = df[["account_id","call_id"]]
df_callr["recording_url"] = ""
df_callr = df_callr.astype(str)

#Define Function to get Call Recording from Call Id
def get_call_recording(row):
    
    try:
        row["recording_url"] = requests.get(callr_ep, headers = {"Authorization":"Bearer {}".format(user_tokens[row["account_id"]])},
                        params={"call_id":row["call_id"],"include_recording":1}).json()["call"]["call"]["recording_url"]
    except:
        row["recording_url"] = np.nan
        
    return row

#Apply funtion to DataFrame
start = time.time()
df_callr = df_callr.apply(get_call_recording, axis=1)
end = time.time()   

print(end-start)

####################### GET CONTACT INFORMATION ##############################

#Data to extract

data = {"contact_id":[],
        "first_name":[],
        "last_name":[],
        "lead_name":[],
        "company":[],
        "title":[],
        "primary_phone":[],
        "primary_email":[],
        "lead_tags":[],
        "notes":[],
        "total_calls":[],
        "zoho_id":[]
        }
cid_list = list(df['user_id'].unique())
#request parameters
contacts_ep ="https://www.phoneburner.com/rest/1/contacts"

#loop to extract contact ids information
start = time.time()
for cid in cid_list:

    contacts_json = requests.get(contacts_ep, headers = {"Authorization":"Bearer {}".format(atoken)}, 
                               params = {"contact_id":cid}).json()["contacts"]["contacts"][0]
    try:
        data["contact_id"].append(cid)
    except:
        data["contact_id"].append(np.nan)
    try:
        data["first_name"].append(contacts_json["first_name"])
    except:
        data["first_name"].append(np.nan)
    try:
        data["last_name"].append(contacts_json["last_name"])
    except:
        data["last_name"].append(np.nan)
    try:   
        data["lead_name"].append(contacts_json["first_name"]+" "+contacts_json["last_name"])
    except:
        data["lead_name"].append(np.nan)
    try: 
        data["primary_phone"].append(contacts_json["primary_phone"]["phone"])
    except:
        data["primary_phone"].append(np.nan)
    try:
        data["primary_email"].append(contacts_json["primary_email"]["email_address"])
    except:
        data["primary_email"].append(np.nan)
    try:
        data["notes"].append(contacts_json["notes"]["notes"])
    except:
        data["notes"].append(np.nan)
    try:
        data["total_calls"].append(contacts_json["total_calls"])
    except: 
        data["total_calls"].append(np.nan)
        
    if len(contacts_json["custom_fields"]) > 0:
    
        for cfield in contacts_json["custom_fields"]:
        
            if cfield["custom_field_id"] == "401331":
                data["company"].append(cfield["value"])

            if cfield["custom_field_id"] == "403545":
                data["title"].append(cfield["value"])

            if cfield["custom_field_id"] == "442680":
                data["lead_tags"].append(cfield["value"])

            if cfield["custom_field_id"] == "422976":
                data["zoho_id"].append(cfield["value"])    
                
    if len(data["company"]) != len(data["contact_id"]):
        data["company"].append(np.nan)
        
    if len(data["title"]) != len(data["contact_id"]):    
        data["title"].append(np.nan)
        
    if len(data["lead_tags"]) != len(data["contact_id"]): 
        data["lead_tags"].append(np.nan)
        
    if len(data["zoho_id"]) != len(data["contact_id"]): 
        data["zoho_id"].append(np.nan)
        
#Dataframe to store the data extracted
df_contact_info =  pd.DataFrame(data=data)                
    
end = time.time()

print(end-start)   

#conert df into excel
df_contact_info.to_excel('Contacts_Called_since_aug_2021.xlsx')






