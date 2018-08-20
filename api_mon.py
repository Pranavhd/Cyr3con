#!/usr/bin/env python


import requests
from datetime import datetime, date, timedelta
from operator import itemgetter
from configparser import ConfigParser
import smtplib
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import time
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine
import datetime
import pymongo
import os
import boto3
from botocore.exceptions import ClientError
from boto.ses.connection import SESConnection

start_time = time.time()

config = ConfigParser()

#config file used to load all credentials
#config.read('/home/ubuntu/mountDir/api_mongo_test/final_config.ini')

username = os.environ['API_USER']  # Get username from config.ini file
#username = config.get('UserDetails','username')  # Get username from config.ini file
password = os.environ['API_PASSWORD']  # Get password from config.ini file
#password = config.get('UserDetails','password')  # Get password from config.ini file

MONGO_SERVER_IP =os.environ['MONGO_HOST']
mongo_username =os.environ['MONGO_USERNAME']
mongo_password =os.environ['MONGO_PASSWORD']

conn = pymongo.MongoClient(MONGO_SERVER_IP)
conn.admin.authenticate(mongo_username ,mongo_password , mechanism='SCRAM-SHA-1')
database = conn.Simba

# Global Parameters
headers = None
limit = config.get('UserDetails','limit')  # Set limit to our API call
limit_hacking_threads = config.get('UserDetails','limit_hacking_threads')
today = datetime.datetime.today().strftime('%Y-%m-%d')

fixeddays = int(config.get('Data','fixeddays'))
date_list = []
'''
i = 190
while i>0:
    date_list.append(i)
    i-=60
date_list.append(2)
'''
date_list.append(fixeddays)

dict_api = dict()
dict_api_to_coll = dict()
flag_status = 0
dict_status = dict()

#for each api create a dictionary to map the api short name (ghi) to url (https://abc.com/def/ghi)
dict_api_to_coll['']=''

string_list_message = ""
list_message = []
temp_list = ['API NAME']
for i in range(len(date_list)):
    temp_list.append('API ' + str(date_list[i]) + ' days')
    temp_list.append('Mongo ' + str(date_list[i]) + ' days')
list_message.append(temp_list)

#all the apis
list_of_api = [
    ''
		]

def initiate_headers():
    global headers
    call = r"https://example.com/other/login"
    credentials = {'userId':username,'password':password}
    response = requests.post(call,headers = credentials)
    #response = requests.post(call)
    headers = {"userId": username, "apiKey": response.json()['apiKey']}
    pass

def run_api_getcount(url):
    global headers
    if not headers:
        initiate_headers()
    try:
        response = requests.get(url, headers=headers)
        if response.status_code==200:
            return response.json()['count']
        else:
            return "Status_not_200"
    except:
        print(response.json())
        if response.json()['error'] == "Unauthorized":
            initiate_headers()
            response = requests.get(url, headers=headers)
    return response.json()['results']

def run_api(url):
    global headers

    if not headers:
        initiate_headers()
    try:
        response = requests.get(url, headers=headers)
        if response.status_code==200:
            return response.json()['results']
        else:
            return "Status_not_200"
    except:
        print response.json()
        if response.json()['error'] == "Unauthorized":
            initiate_headers()
            response = requests.get(url, headers=headers)
    return response.json()['results']

def get_time(i):
    return date.today()-timedelta(int(i))

for each_api in list_of_api:
    dict_api[each_api]=[]
    for each_date in date_list:
        days_before = get_time(each_date)
        limited_api = each_api + '?limit=' + str(limit) + '&from=' +str(days_before)
        print limited_api
        results = run_api(limited_api)

        if results == "Status_not_200":
            dict_api[each_api].append("Status not 200")
	    dict_status[each_api]=[]
	    dict_status[each_api].append("Status not 200")
	    flag_status = 1
            continue

        counter = 0
        for each_result in results:
            if str(each_result['parameter'])>str(days_before):
                counter += 1
        dict_api[each_api].append(counter)

	collection = database[dict_api_to_coll[each_api]]
	mongo_value = collection.find({'scraped_date':{'$gte': datetime.datetime.now()-timedelta(days=fixeddays)}}).count()
	dict_api[each_api].append(int(mongo_value))


#if collections have different structure
#{ "scraped_datetime" : { "$gte" : { "$date" : "2018-06-12T07:00:00.000Z"}}}, Fields: null, Sort: { "recorded_time_seconds" : -1}
collection = database['pqr']
res = collection.find({'recorded_time_seconds':{'$gte': datetime.datetime.today()-timedelta(fixeddays)}}).count()
dict_api[dict_api_to_coll['pqr']].append(res)

'''
manually check if data value < a given threshold
for each key in dict_api:
    if each key == abc:
        if api_count < xyz or mongo_count < uvw:
	    dict_status[each key] = []
	    dict_status[each key].append(api_count)
	    dict_status[each key].append(mongo_count)
'''
#user = config.get('Mysql','username')
#psswd = config.get('Mysql','password')
#db = config.get('Mysql','dbname')
#ip = config.get('Mysql','ip')

user = os.environ['SQL_USER']
psswd = os.environ['SQL_PASSWORD']
db = os.environ['SQL_DBNAME']
ip = os.environ['SQL_HOST']

engine = create_engine('mysql://'+user+':'+psswd+'@' + ip + '/'+db)
for key in dict_api:
    index = -1
    for ind, character in enumerate(key):
        if character == '/':
            index = ind
    db_api_name = key[index+1:]
    db_date = str(date.today())
    temp_list = dict_api[key]

    if temp_list[0]!='Status not 200':
        api_count =int(temp_list[0])
    if len(temp_list)==1:
	mongo_count = -1
	dict_api[key].append(mongo_count)
    else:
	mongo_count = int(temp_list[1])
    db_record = {'date':[db_date],'api_count':[api_count],'mongo_count':[mongo_count]}
    df = pd.DataFrame(data=db_record)

    with engine.connect() as econn, econn.begin():
        df.to_sql(db_api_name, econn, if_exists='append', index = False)

'''
#append dict_api to list_message for daily stats
for key in dict_api:
    temp_list = []
    temp_list.append(key)
    for each_ele in dict_api[key]:
        temp_list.append(each_ele)
    list_message.append(temp_list)
'''

#for only specific alerts
for key in dict_api:
    temp_list = []
    add_alert = 0
    temp_list.append(key)
    for each_ele in dict_api[key]:
	if each_ele == 0 or each_ele == 'Status not 200':
	    add_alert = 1
    if add_alert==1:
	for each_ele in dict_api[key]:
	    temp_list.append(each_ele)
	list_message.append(temp_list)

for each_list in list_message:
    inde = 0
    for each_ele in each_list:
	#if inde==0:
        #string_list_message += str(each_ele) + "\t"
        #    inde += 1
	#else:
	string_list_message += str(each_ele) + "::\t::"
    string_list_message += "\n"

html = """
<html><body>
<p>Below are the Statistics, NOTE:5000 is the maximum limit of API records returned</p>
{table}
</body></html>
"""
data = list_message
html = html.format(table=tabulate(data, headers="firstrow", tablefmt="html"))
message = MIMEMultipart("alternative", None, [MIMEText(html,'html')])

def send_email_table(message):
    smtp_ssl_host = 'smtp.' + config.get('EmailDetails','ssl_host') + '.com'  # smtp.mail.yahoo.com
    smtp_ssl_port = 465
    username = config.get('EmailDetails','username')
    password = config.get('EmailDetails','password')
    sender = config.get('EmailDetails','sender')
    targets = [config.get('EmailDetails','targets')]

    other_targets = list(config.items('OtherEmail'))
    for element in other_targets:
        targets.append(element[0])

    msg = message
    msg['Subject'] = 'Status Alert of API and Mongo'
    msg['From'] = sender
    msg['To'] = ', '.join(targets)

    server = smtplib.SMTP_SSL(smtp_ssl_host, smtp_ssl_port)
    server.login(username, password)
    server.sendmail(sender, targets, msg.as_string())
    server.quit()

def send_email_table_aws(message):
    SENDER = "info@abc.com"
    # the key to access the AWS server

    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_KEY']

    RECIPIENT = ["lpl@gmail.com"]

    CONFIGURATION_SET = "ConfigSet"

    AWS_REGION = "us-west-2"

    SUBJECT = "API Mongo Count"

    BODY_TEXT = message

    # The character encoding for the email.
    CHARSET = "us-ascii"

    # Create a new SES resource and specify a region.
    # AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY

    client = boto3.client('ses', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                          region_name=AWS_REGION)

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses':
                    RECIPIENT,

            },
            Source=SENDER,
            Message={
                'Subject': {
                    'Data': SUBJECT,
                    'Charset': 'us-ascii'
                },
                'Body': {
                    'Text': {
                        'Data': BODY_TEXT,
                        'Charset': 'us-ascii'

                    }
                }
            })
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

#send_email_table(message)

for key in dict_api:
    print key, dict_api[key]
send_email_table_aws(string_list_message)
print "--- %s seconds ---" % (time.time() - start_time)
