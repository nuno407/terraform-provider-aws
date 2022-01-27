import requests

url = "http://a1323b91e580241648f511e4ff7e7070-788135993.eu-central-1.elb.amazonaws.com/getQueryItems"

payload={}
files={}
headers = {
  'Cookie': 'BCSI-CS-03695f491acef09d=2'
}

response = requests.request("GET", url, headers=headers, data=payload, files=files)

print(response)
