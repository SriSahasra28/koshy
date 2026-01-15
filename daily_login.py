import os
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)
print(os.getcwd())
from datetime import datetime
from background.login import login
from background.utils import utils
from background.database import DBHelper
import redis
u = utils()

zerodha_login_status = False

l = login(False)
zerodha_login_status, kite, kws, token = l.InitiateZerodha()

message = f"koshy {zerodha_login_status=}"

# if zerodha_login_status == True:
#     u.send_email(f"Logged in successfully ", message)
# else:
#     u.send_email("Login Failure", message)

print(message)
cur_date = datetime.today().date()

DBHelper.update_access_token(token, cur_date)
print('access token saved in database')

#l.download_instruments('NSE')
l.download_instruments('NFO')

DBHelper.run_query('Call Resetdb()')
print('Database Koshy Reset')

# redis_client = redis.Redis(host='localhost', port=6379, db=0)
# redis_client.flushall()

file_path = "C:\\Users\\Administrator\\Desktop\\React\\koshy-trading-app-server\\.env"

text_to_save = f"""
API_KEY=njkendkywo49rhna
ACCESS_TOKEN= "{token}"
"""

with open(file_path, 'w') as file:
    file.write(text_to_save)

print("Token saved successfully to:", file_path)