import os
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)
print(os.getcwd())
from datetime import datetime
from background.database import DBHelper

db = DBHelper()

df_cred = db.get_credentials()
token = df_cred['access_code'].iloc[0]
#print(f"{token=}")

file_path = 'C:\\Users\\Administrator\\Desktop\\React\\koshy-trading-app\\koshy-trading-app-server//.env'

text_to_save = f"""
API_KEY=njkendkywo49rhna
ACCESS_TOKEN= "{token}"
"""

with open(file_path, 'w') as file:
    file.write(text_to_save)

print("Token saved successfully to:", file_path)