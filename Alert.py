import elasticsearch
import datetime
from dateutil import parser
import re
import hashlib

#creating Elasticsearch connection:
es_ag =elasticsearch.Elasticsearch("http://elastic:changeme@10.350.180.135:9200")
print("Connection started")

if es_ag.indices.exists(index="alert_analysis"):
   es_ag.indices.delete(index='alert_analysis')
   print ("Index Deleted")


def lclean(x,loc=1):
	return str(x)[loc:]

def rclean(x,loc=1):
	return str(x)[:-loc]
	
i=0
#opening the CSV file
try:
	with open("Alert.csv") as f:
		for line in f:
			value=line.split("\",\"")
			created_date = parser.parse(lclean(value[0]))
			asso_ticket = value[1]
			alarm_group = value[2]
			sub_method = value[3]
			check_title = value[4]
			summary = value[5]
			severity = value[6]
			business_service = value[7]
			assigned_group = rclean(value[8].strip())
			alarm_id= hashlib.md5(value[0]+value[1]+value[2]+value[3]+value[4]+value[5]+value[6]+value[7]+value[8]).hexdigest()
			
			if ('APPS' in asso_ticket):
				asso_ticket_type = "APPS"
			elif ('CHG' in asso_ticket):
				asso_ticket_type = "CHANGE"
			elif ('INC' in asso_ticket):
				asso_ticket_type = "INCIDENT"
			else:
				asso_ticket_type = "Unknown"
			
			
			if ('Production' in check_title or 'Production' in summary or 'PRD' in check_title or 'PRD' in summary or 'production' in check_title or 'production' in summary or 'prd' in check_title or 'prd' in summary):
				environment='PROD'
			elif ('uat' in check_title or 'uat' in summary or 'UAT' in check_title or 'UAT' in summary):
				environment='UAT'
			elif ('training' in check_title or 'training' in summary or 'TRN' in check_title or 'TRN' in summary or 'trn' in check_title or 'trn' in summary):
				environment='TRN'
			else:
				environment='Unknown'
			i=i+1
			
			print `i`+ " " + environment +" "+ summary + " " + check_title
			
				
			

			
			es_ag.index(index='alert_analysis', doc_type='alert_analysis', id=alarm_id, body={'date': created_date, 'associated_ticket': asso_ticket, 'alarm_group': alarm_group, 'sub_method': sub_method, 'check_title':check_title, 'summary':summary, 'severity': severity, 'business_service': business_service, 'assigned_group': assigned_group, 'associated_ticket_type':asso_ticket_type,'environment':environment})
			
	print ("SUCCESS")
			
except:
	print ("Error")
