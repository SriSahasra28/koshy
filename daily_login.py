import os
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)
print(os.getcwd())

from background.login import login
from background.utils import utils
from background.database import DBHelper
u = utils()

zerodha_login_status = False

l = login(False)
zerodha_login_status, kite, kws = l.InitiateZerodha()

message = f"koshy {zerodha_login_status=}"

if zerodha_login_status == True:
    u.send_email(f"Logged in successfully ", message)
else:
    u.send_email("Login Failure", message)

print(message)

#l.download_instruments('NSE')
#l.download_instruments('NFO')

# DBHelper.run_query('Call Resetdb()')
# print('Database Jiva Reset')

# directory_path = 'data'  # Replace with the path to your directory
# files = os.listdir(directory_path)
# for file in files:
#     if file.endswith('.csv'):
#         file_path = os.path.join(directory_path, file)
#         os.remove(file_path)
#         print(f"Deleted: {file_path}")