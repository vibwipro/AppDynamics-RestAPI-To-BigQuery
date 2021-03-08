#**************************************************************************
#Script Name    : Gen_60Min_data.py
#Description    : This script will collect last 60 minute data and that will considered for loading BQ-table
#Created by     : Vibhor Gupta
#Version        Author          Created Date    Comments
#1.0            Vibhor          2020-07-29      Initial version
#***************************************************************************

import requests, json, sys
data_file=sys.argv[1]
service_name=sys.argv[2]
##############Token Generatin ##############################################
token_url = "https://projprod.saas.appdynamics.com/controller/api/oauth/access_token"
token_body = "grant_type=client_credentials&client_id=prediction_team@projprod&client_secret=abcde-eb123-213-910d-eed3we345d337"
req= requests.post(token_url, data=token_body)
data= req.json()
access_token= data['access_token']
###########################################################################
url_performance = "https://projprod.saas.appdynamics.com/controller/rest/applications/Database%20Monitoring/metric-data?metric-path=Databases%7C" + str(service_name) + "%7CPerformance%7C*&time-range-type=BEFORE_NOW&duration-in-mins=60&rollup=true&output=json"
##############Pulling Performance Data Using Token generated above#######################
pwd = 'Bearer ' + access_token
url = url_performance
headers={
    'authorization':pwd
    }
req = requests.get(url, headers=headers)
data = req.json()
############################################3
for Up_list in data:
    Up_dict = Up_list['metricValues']
    frequency = Up_list['frequency']
    metricId = Up_list['metricId']
    f = open(data_file, "a")

    for In_list  in Up_dict:
        #f.write(str(metricId)+ ', ' + str(In_list).replace("u'", "'").replace("{", "").replace("}", "\n"))
        f.write(str(service_name) + ', ' + str(metricId) + ', ' + str(frequency) + ', ' + str(In_list['useRange'])+ ', ' + str(In_list['count'])+ ', ' + str(In_list['min'])+ ', ' + str(In_list['max'])+ ', ' + str(In_list['sum'])+ ', ' + str(In_list['standardDeviation'])+ ', ' + str(In_list['value'])+ ', ' + str(In_list['current'])+ ', ' + str(In_list['occurrences'])+ ', ' + str(In_list['startTimeInMillis']) + "\n")

    f.close()

###########################################################################
url_kpi = "https://projprod.saas.appdynamics.com/controller/rest/applications/Database%20Monitoring/metric-data?metric-path=Databases%7C" + str(service_name) + "%7CKPI%7C*&time-range-type=BEFORE_NOW&duration-in-mins=60&rollup=true&output=json"
##############Pulling KPI Data Using Token generated above#######################

pwd = 'Bearer ' + access_token
url = url_kpi
headers={
    'authorization':pwd
    }
req = requests.get(url, headers=headers)
data = req.json()
############################################3
for Up_list in data:
    Up_dict = Up_list['metricValues']
    frequency = Up_list['frequency']
    metricId = Up_list['metricId']
    f = open(data_file, "a")

    for In_list  in Up_dict:
        #f.write(str(metricId)+ ', ' + str(In_list).replace("u'", "'").replace("{", "").replace("}", "\n"))
        f.write(str(service_name) + ', ' + str(metricId) + ', ' + str(frequency) + ', ' + str(In_list['useRange'])+ ', ' + str(In_list['count'])+ ', ' + str(In_list['min'])+ ', ' + str(In_list['max'])+ ', ' + str(In_list['sum'])+ ', ' + str(In_list['standardDeviation'])+ ', ' + str(In_list['value'])+ ', ' + str(In_list['current'])+ ', ' + str(In_list['occurrences'])+ ', ' + str(In_list['startTimeInMillis']) + "\n")

    f.close()

###########################################################################
url_server = "https://projprod.saas.appdynamics.com/controller/rest/applications/Database%20Monitoring/metric-data?metric-path=Databases%7C" + str(service_name) + "%7CServer Statistic%7C*&time-range-type=BEFORE_NOW&duration-in-mins=60&rollup=true&output=json"
##############Pulling Server Statistic Data Using Token generated above#######################
pwd = 'Bearer ' + access_token
url = url_server
headers={
    'authorization':pwd
    }
req = requests.get(url, headers=headers)
data = req.json()
############################################3
for Up_list in data:
    Up_dict = Up_list['metricValues']
    frequency = Up_list['frequency']
    metricId = Up_list['metricId']
    f = open(data_file, "a")

    for In_list  in Up_dict:
        #f.write(str(metricId)+ ', ' + str(In_list).replace("u'", "'").replace("{", "").replace("}", "\n"))
        f.write(str(service_name) + ', ' + str(metricId) + ', ' + str(frequency) + ', ' + str(In_list['useRange'])+ ', ' + str(In_list['count'])+ ', ' + str(In_list['min'])+ ', ' + str(In_list['max'])+ ', ' + str(In_list['sum'])+ ', ' + str(In_list['standardDeviation'])+ ', ' + str(In_list['value'])+ ', ' + str(In_list['current'])+ ', ' + str(In_list['occurrences'])+ ', ' + str(In_list['startTimeInMillis']) + "\n")

    f.close()

