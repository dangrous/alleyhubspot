import requests
import json
import csv
import sys
from datetime import datetime

api_key = sys.argv[1]
total_requests = 0

# IMPORTANT! We need to make sure the dates of the deal are AFTER the dates of the interaction. poop.

def get_touchpoints(api_key):

	global total_requests

	touchpoint_value = {}
	final_values = {}

	pipeline_data = requests.get('https://api.hubapi.com/crm/v3/pipelines/deals?archived=false&hapikey={}'.format(api_key)).json()
	total_requests += 1

	pipelines = {}
	stages = {}

	columns = ['touchpoint','total','number']

	for pipeline in pipeline_data['results']:

		pipelines[pipeline['id']] = {'name': pipeline['label'], 'stages': {}}

		for stage in pipeline['stages']:
			pipelines[pipeline['id']]['stages'][stage['id']] = pipeline['label'] + ' - ' + stage['label']
			stages[stage['id']] = pipeline['label'] + ' - ' + stage['label']
			columns.append("{} - {}".format(pipeline['label'],stage['label']))

	data = requests.get('https://api.hubapi.com/crm/v3/properties/contact/events_attended?archived=false&hapikey={}'.format(api_key)).json()
	total_requests += 1

	touchpoints = {}
	contacts = {}
	all_deals = {}

	for touchpoint in data['options']:
		
		if touchpoint['value']:

			print('Looking at {}'.format(touchpoint['value']))
			
			influenced_deals = {}

			touchpoint_value[touchpoint['value']] = {}

			for pipeline in pipelines:
				for stage in pipelines[pipeline]['stages']:
					touchpoint_value[touchpoint['value']][pipelines[pipeline]['stages'][stage]] = []

			first_contacts_call = True

			influenced = {}

			after_contacts = '0'

			while first_contacts_call or 'paging' in influenced:

				payload = {'properties' : [ 'email', 'events_attended', 'firstname'],'limit' : '100','after' : after_contacts, 'filterGroups' : [{'filters': [{'propertyName' : 'events_attended', 'operator' : 'CONTAINS_TOKEN', 'value' : touchpoint['value']}]}]}
		
				headers = {'Content-Type' : 'application/json'}

				influenced = requests.post('https://api.hubapi.com/crm/v3/objects/contacts/search?hapikey={}'.format(api_key), data = json.dumps(payload), headers = headers).json()
				total_requests += 1

				touchpoints['value'] = influenced

				for contact in influenced['results']:

					if contact['id'] in contacts:

						associated_deals = contacts[contact['id']]

					else:

						payload = {'inputs' : [ { 'id' : contact['id'] } ] }

						headers = {'Content-Type' : 'application/json'}

						associated_deals = requests.post('https://api.hubapi.com/crm/v3/associations/contacts/deals/batch/read?hapikey={}'.format(api_key), data = json.dumps(payload), headers = headers).json()
						total_requests += 1

						contacts[contact['id']] = associated_deals

					for deal_list in associated_deals['results']:

						for deal in deal_list['to']:

							deal_id = deal['id']

							if deal_id in influenced_deals:

								pass

							elif deal_id in all_deals:

								influenced_deals[deal_id] = all_deals[deal_id]

							else:

								after_deals = '0'

								first_deals_call = True

								while first_deals_call or 'paging' in deal_data:

									deal_data = requests.get('https://api.hubapi.com/crm/v3/objects/deals/{}?archived=false&hapikey={}'.format(deal['id'],api_key)).json()
									total_requests += 1

									all_deals[deal_id] = deal_data

									influenced_deals[deal_id] = deal_data

									first_deals_call = False

									if 'paging' in deal_data:
										after_deals = deal_data['paging']['next']['after']

				first_contacts_call = False

				if 'paging' in influenced:
					after_contacts = influenced['paging']['next']['after']

			for deal in influenced_deals:

				if influenced_deals[deal]['properties']['amount'] != None and influenced_deals[deal]['properties']['amount'] != '':
					touchpoint_value[touchpoint['value']][stages[influenced_deals[deal]['properties']['dealstage']]].append(int(influenced_deals[deal]['properties']['amount']))

			final_values[touchpoint['value']] = {}
			final_values[touchpoint['value']]['number'] = 0
			final_values[touchpoint['value']]['total'] = 0

			for stage in touchpoint_value[touchpoint['value']]:
				if touchpoint_value[touchpoint['value']][stage] != []:
					final_values[touchpoint['value']][stage] = 0
					for amount in touchpoint_value[touchpoint['value']][stage]:
						final_values[touchpoint['value']][stage] += amount
						final_values[touchpoint['value']]['total'] += amount
						final_values[touchpoint['value']]['number'] += 1

	print(total_requests)

	with open('influenced_value.csv', mode='w') as csv_file:
		writer = csv.DictWriter(csv_file, fieldnames=columns)

		writer.writeheader()
		
		for touchpoint in final_values:
			row = {}

			row['touchpoint'] = touchpoint

			for key, value in final_values[touchpoint].items():
				row[key] = value

			writer.writerow(row)

def email_interactions_to_deals(api_key):

	global total_requests

	all_contacts = requests.get('https://api.hubapi.com/crm/v3/objects/contacts?limit=10&archived=false&hapikey={}'.format(api_key)).json()
	total_requests += 1

	for contact in all_contacts['results']:
		email_interactions = requests.get('https://api.hubapi.com/email/public/v1/events?hapikey={}&recipient={}'.format(api_key, contact['properties']['email'])).json()
		total_requests += 1
		print(json.dumps(email_interactions))

	print(total_requests)

def lead_flow(api_key):

	global total_requests

	average = []

	has_more = True

	offset = 0

	while (has_more or 'hasMore' in all_deals) and total_requests < 500:

		all_deals = requests.get('https://api.hubapi.com/deals/v1/deal/paged?hapikey={}&includeAssociations=false&offset={}&limit=10&propertiesWithHistory=dealstage'.format(api_key,offset)).json()
		total_requests += 1

		has_more = False

		if 'hasMore' in all_deals:
			offset = all_deals['offset']

		for deal in all_deals['deals']:

			deal_create = datetime.fromtimestamp(deal['properties']['dealstage']['versions'][-1]['timestamp']/1000)

			payload = {'inputs' : [ { 'id' : deal['dealId'] } ] }

			headers = {'Content-Type' : 'application/json'}

			deal_contacts = requests.post('https://api.hubapi.com/crm/v3/associations/deals/contacts/batch/read?hapikey={}'.format(api_key), data = json.dumps(payload), headers = headers).json()
			total_requests += 1

			for contact in deal_contacts['results']:

				for association in contact['to']:

					contact_date = requests.get('https://api.hubapi.com/crm/v3/objects/contacts/{}?archived=false&hapikey={}'.format(association['id'],api_key)).json()
					total_requests += 1

					date_object = datetime.strptime(contact_date['properties']['createdate'][:-14], '%Y-%m-%d')

					if contact:

						difference = deal_create - date_object

						print(contact_date['properties']['email'] + " was created at " + str(date_object) + " and the deal was created at " + str(deal_create))

						print("Time between contact creation and deal creation: " + str(difference))

						average.append(difference)

	print(mean(average))
		
get_touchpoints(api_key)
# email_interactions_to_deals(api_key)
# lead_flow(api_key)

print(total_requests)