import requests
import json
import csv
import sys
from datetime import datetime

# Pulls in the first command line argument as the API key
api_key = sys.argv[1]

# Keeps track of how many API requests I'm making - at some point may want to add a limit if it gets too high.
total_requests = 0

# This function pulls all the possible touchpoints, finds contacts that were there, finds deals associated
# with those contacts, and spits out a csv of the total (potential) influenced revenue, separated by deal stage
def get_touchpoints(api_key):

	# Pulls in the global variable to be able to track requests across multiple functions
	global total_requests

	# Created dictionaries to hold the results from the requests and calculation. Might not need these?
	touchpoint_value = {}
	final_values = {}

	# Requests all the pipelines (and stages) from Hubspot. Can potentially abstract this out into a requests function to be reused
	pipeline_data = requests.get('https://api.hubapi.com/crm/v3/pipelines/deals?archived=false&hapikey={}'.format(api_key)).json()
	total_requests += 1

	# Dictionaries for storing the pipelines and stages. I don't think I actually use stages - confirm and delete if so
	pipelines = {}
	stages = {}

	# Prepping the columns for the CSV creation later in the function
	columns = ['touchpoint','total','number']

	# This pulls the stages from the Hubspot request and organizes it appropriately in the previous dicts
	for pipeline in pipeline_data['results']:
		pipelines[pipeline['id']] = {'name': pipeline['label'], 'stages': {}}

		# This adds the pipeline to the stage name because otherwise there are dupes between the pipelines
		for stage in pipeline['stages']:
			pipelines[pipeline['id']]['stages'][stage['id']] = pipeline['label'] + ' - ' + stage['label']
			stages[stage['id']] = pipeline['label'] + ' - ' + stage['label']
			columns.append("{} - {}".format(pipeline['label'],stage['label']))

	# This pulls all the possible touchpoints from Hubspot - it does not appear to have pagination
	data = requests.get('https://api.hubapi.com/crm/v3/properties/contact/events_attended?archived=false&hapikey={}'.format(api_key)).json()
	total_requests += 1

	# Dictionaries for storing and organizing the data from Hubspot. Attempting to use these to limit calls (e.g contacts w/more than one touchpoint or deal)
	contacts = {}
	all_deals = {}

	# This loop takes each touchpoint, finds all contacts with that touchpoint, and finds all deals associated with those touchpoints
	for touchpoint in data['options']:

		# There's one blank touchpoint that I can't figure out how to get rid of, hence this if		
		if touchpoint['value']:
			
			# Status update in Terminal!
			print('Looking at {}'.format(touchpoint['value']))
			
			# Creating a new list of deals and total value for each touchpoint
			influenced_deals = {}
			touchpoint_value[touchpoint['value']] = {}

			# Setting up the value to separate out deals by stages
			for pipeline in pipelines:
				for stage in pipelines[pipeline]['stages']:
					touchpoint_value[touchpoint['value']][pipelines[pipeline]['stages'][stage]] = []

			# Prepping the while loop below to deal with pagination
			first_contacts_call = True
			influenced = {}
			after_contacts = '0'

			# This will keep looping until there are no more results ('paging' in the response means there are more)
			while first_contacts_call or 'paging' in influenced:
				
				# This builds out and sends the data for the request using the appropriate touchpoint name
				payload = {'properties' : [ 'email', 'events_attended', 'firstname'],'limit' : '100','after' : after_contacts, 'filterGroups' : [{'filters': [{'propertyName' : 'events_attended', 'operator' : 'CONTAINS_TOKEN', 'value' : touchpoint['value']}]}]}
				headers = {'Content-Type' : 'application/json'}
				influenced = requests.post('https://api.hubapi.com/crm/v3/objects/contacts/search?hapikey={}'.format(api_key), data = json.dumps(payload), headers = headers).json()
				total_requests += 1

				# Looping through the matched contacts, finding their associated deals, and adding the deals to the influenced_deals dict 
				for contact in influenced['results']:

					# If we already have pulled the data for this contact, don't do it again, just copy it from the dict
					if contact['id'] in contacts:
						associated_deals = contacts[contact['id']]

					else:

						# Pull all deals associated with this contact and add to the dict
						payload = {'inputs' : [ { 'id' : contact['id'] } ] }
						headers = {'Content-Type' : 'application/json'}
						associated_deals = requests.post('https://api.hubapi.com/crm/v3/associations/contacts/deals/batch/read?hapikey={}'.format(api_key), data = json.dumps(payload), headers = headers).json()
						total_requests += 1
						contacts[contact['id']] = associated_deals

					# Get the details for all the associated deals
					for deal_list in associated_deals['results']:
						for deal in deal_list['to']:
							deal_id = deal['id']

							# This logic prevents us from potentially double counting deals within a touchpoint, and also saves calls by pulling from the dict
							if deal_id in influenced_deals:
								pass
							elif deal_id in all_deals:
								influenced_deals[deal_id] = all_deals[deal_id]
							else:

								# Prep for and execute while loop for getting the deal data and storing it. Same pagination stuff as the contacts
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

			# Once we've gotten all the deals, add their value into the list for the appropriate deal stage in the touchpoint. This currently only adds deals with value
			for deal in influenced_deals:
				if influenced_deals[deal]['properties']['amount'] != None and influenced_deals[deal]['properties']['amount'] != '':
					touchpoint_value[touchpoint['value']][stages[influenced_deals[deal]['properties']['dealstage']]].append(int(influenced_deals[deal]['properties']['amount']))

			# This is putting the actual data that gets printed into its final form. I think I can combine this with touchpoint_value
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

	# Status update in Terminal for how we're doing with the requests 
	print(total_requests)

	# Building out the CSV. This might or might not get dropped in favor of going directly to Google Data Studio
	with open('influenced_value.csv', mode='w') as csv_file:
		writer = csv.DictWriter(csv_file, fieldnames=columns)
		writer.writeheader()
		for touchpoint in final_values:
			row = {}
			row['touchpoint'] = touchpoint
			for key, value in final_values[touchpoint].items():
				row[key] = value
			writer.writerow(row)

# [TO BE IMPLEMENTED] This pulls the interactions correctly but is not analyzed at all - it's going to be a lot of calls, too.
def email_interactions_to_deals(api_key):

	global total_requests

	all_contacts = requests.get('https://api.hubapi.com/crm/v3/objects/contacts?limit=10&archived=false&hapikey={}'.format(api_key)).json()
	total_requests += 1

	for contact in all_contacts['results']:
		email_interactions = requests.get('https://api.hubapi.com/email/public/v1/events?hapikey={}&recipient={}'.format(api_key, contact['properties']['email'])).json()
		total_requests += 1
		print(json.dumps(email_interactions))

	print(total_requests)

# [TO BE IMPLEMENTED] This pulls all deals from Hubspot and all history associated with them. It is a ton of calls already, need to figure out how to streamline
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
		
# Actually get the data
get_touchpoints(api_key)
# email_interactions_to_deals(api_key)
# lead_flow(api_key)

# Report on total requests across all three pulls, to see how much I'm slamming the API
print(total_requests)
