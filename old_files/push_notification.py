import http.client, urllib
conn = http.client.HTTPSConnection("api.pushover.net:443")
conn.request("POST", "/1/messages.json",
  urllib.parse.urlencode({
    "token": "a1nxiqkvd7y7ez7pk9bdptfqqqhdmy",
    "user": "uo5kismt466cugxrn1gi82eb965mng",
    "title":"INFY24AUG1800CE",
    "message": "new alert",
    'url': "http://103.160.145.141/"
  }), { "Content-type": "application/x-www-form-urlencoded" })
conn.getresponse()