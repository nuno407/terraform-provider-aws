
import requests


url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"_id\": {'==' :\"pipeline_check_final\"}}/or"
request_response = requests.get(url)
status_code = request_response.status_code
print("Valid test - expected status code 200 : Valid Collection + 1 Valid parameter:string + Valid operator")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"has\": \"CHC\"},\"_id\": { \"==\": \"pipeline_check_final\"},\"from_container\": { \"==\": \"Metadata\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Valid test - expected status code 200 : Valid Collection + 3 Valid parameter:string + Valid operator")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-invalid-collection/{\"_id\": {'==' :\"pipeline_check_final\"}}/or"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 :  Invalid Collection")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"invalid\": \"CHC\"},\"_id\": { \"==\": \"pipeline_check_final\"},\"from_container\": { \"==\": \"Metadata\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : Invalid parameter:string operator (beginning)s")
print("Response: ")
print(request_response.json())
print()
	

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"invalid\": { \"has\": \"CHC\"},\"_id\": { \"==\": \"pipeline_check_final\"},\"from_container\": { \"==\": \"Metadata\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : Invalid parameter (beginning)")
print("Response: ")
print(request_response.json())
print()
	
url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"has\": \"CHC\"},\"_id\": { \"invalid\": \"pipeline_check_final\"},\"from_container\": { \"==\": \"Metadata\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : Invalid parameter:string operator (middle)")
print("Response: ")
print(request_response.json())
print()
	
url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"has\": \"CHC\"},\"invalid\": { \"==\": \"pipeline_check_final\"},\"from_container\": { \"==\": \"Metadata\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : Invalid parameter (middle)")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"has\": \"CHC\"},\"_id\": { \"==\": \"pipeline_check_final\"},\"from_container\": { \"invalid\": \"Metadata\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : Invalid parameter:string operator (end)")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"has\": \"CHC\"},\"_id\": { \"==\": \"pipeline_check_final\"},\"invalid\": { \"==\": \"Metadata\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : Invalid parameter (end)")
print("Response: ")
print(request_response.json())
print()
	
url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"has\": \"$$$$\"},\"_id\": { \"==\": \"pipeline_check_final\"},\"from_container\": { \"==\": \"Metadata\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : Invalid Value (beginning)")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"has\": \"CHC\"},\"_id\": { \"==\": \"$$$$\"},\"from_container\": { \"==\": \"Metadata\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : Invalid Value (middle)")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"has\": \"CHC\"},\"_id\": { \"==\": \"pipeline_check_final\"},\"from_container\": { \"==\": \"$$$\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : Invalid Value (end)")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"has\": \"CHC\"},\"_id\": { \"==\": \"pipeline_check_final\"},\"from_container\": { \"==\": \"a-zA-Z0-9_:.-\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Valid test - expected status code 200 : With allowed characters a-zA-Z0-9_:.- ")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"_id$\": {'==' :\"pipeline_check_final\"}}/or"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : With $ on parameter")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"_id\": {'==' :\"pipeline$_check_final\"}}/or"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : With $ on value")
print("Response: ")
print(request_response.json())
print()


url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"info_source\": { \"has\": \"CHC\"},\"_id\": { \"==\": \"pipeline_check_final\"},\"from_container\": { \"==\": \"Metadata\"}}/and"
request_response = requests.get(url)
status_code = request_response.status_code
print("Valid test - expected status code 200 : Valid Collection + Valid parameter:string with sub-query + parameter:string with sub-string + Valid operator")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution//or"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 404 : Valid Collection + null + Valid operator")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{: {'==' :\"pipeline_check_final\"}}/or"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 400 : Valid Collection + Null:string  + Valid operator")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems//{\"_id\": {'==' :\"pipeline_check_final\"}}/or"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 404 : Null + Valid parameter:string  + Valid operator")
print("Response: ")
print(request_response.json())
print()

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems/dev-pipeline-execution/{\"_id\": {'==' :\"pipeline_check_final\"}}/"
request_response = requests.get(url)
status_code = request_response.status_code
print("Invalid test - expected status code 404 : Valid Collection + Valid parameter:string  + Null")
print("Response: ")
print(request_response.json())
print()