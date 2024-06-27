#functions to communicate with Zerodha API
#from kiteconnect import KiteConnect
import kiteconnect.exceptions as ex
# from six import StringIO, PY2
# from six.moves.urllib.parse import urljoin
from background.database import DBHelper
import datetime
import pandas as pd
import time
from background.login import login
#from background.fake_clock import Clock
class zeroda():
    helper = DBHelper()
    global ClientCode, SDate, file
    accesstoken = ''
    global clock
    def __init__(self, mode, test_date):
        l = login(False)
        self.mode = mode
        #self.clock = Clock(test_date)
        status, self.kite, kws, token = l.InitiateZerodha()
        self.status = status
        #print('zerodha: ', self.getCurrentDateTime())
        
    def initiate(self):
        return self.kite

    def gethistoricaldata(self, token, from_date, to_date, interval):
        try:
            # ---------- Fetch Directly instead of using the wrapper for faster execution refer connect.py code
            records = self.kite.historical_data(token, from_date=from_date, to_date=to_date, interval=interval)
            # --------- records type List
            df = pd.DataFrame(records)
            return df
        except Exception as e:
            # Log Error 
            print("Error getting Data: {}".format(e))
            df = pd.DataFrame()
            # --------Return Error Code instead of blank dataframe
            return df

    def gethistoricaldata_v2(self, token, from_date, to_date, interval):
        #print(f"gethistoricaldata_v2 {from_date=}, {to_date=}")
        try:
            # ---------- Fetch Directly instead of using the wrapper for faster execution refer connect.py code
            records = self.kite.historical_data(token, from_date=from_date, to_date=to_date, interval=interval)
            df = pd.DataFrame(records)
            return 1, df, None
        except Exception as e:
            # Log Error 
            print("Error getting Data: {}".format(e))
            return 0, None, e

    def gethistoricaldata_v3(self, token, from_date, to_date, interval):
        #print(f"gethistoricaldata_v2 {from_date=}, {to_date=}")
        try:
            records = self.kite.historical_data(token, from_date=from_date, to_date=to_date, interval=interval)
            #df = pd.DataFrame(records)
            return 1, records, None
        except Exception as e:
            print("Error getting Data: {}".format(e))
            return 0, None, e

    # def historical_data(self, instrument_token, from_date, to_date, interval, continuous=False, oi=False):
    #     date_string_format = "%Y-%m-%d %H:%M:%S"
    #     from_date_string = from_date.strftime(date_string_format) if type(from_date) == datetime.datetime else from_date
    #     to_date_string = to_date.strftime(date_string_format) if type(to_date) == datetime.datetime else to_date

    #     data = self._get("market.historical",
    #                      url_args={"instrument_token": instrument_token, "interval": interval},
    #                      params={
    #                          "from": from_date_string,
    #                          "to": to_date_string,
    #                          "interval": interval,
    #                          "continuous": 1 if continuous else 0,
    #                          "oi": 1 if oi else 0
    #                      })

    #     return self._format_historical(data)

    # def _format_historical(self, data):
    #     records = []
    #     for d in data["candles"]:
    #         record = {
    #             "date": dateutil.parser.parse(d[0]),
    #             "open": d[1],
    #             "high": d[2],
    #             "low": d[3],
    #             "close": d[4],
    #             "volume": d[5],
    #         }
    #         if len(d) == 7:
    #             record["oi"] = d[6]
    #         records.append(record)

    #     return records

    # def _get(self, route, url_args=None, params=None, is_json=False):
    #     """Alias for sending a GET request."""
    #     return self._request(route, "GET", url_args=url_args, params=params, is_json=is_json)

    # def _request(self, route, method, url_args=None, params=None, is_json=False, query_params=None):
    #     """Make an HTTP request."""
    #     # Form a restful URL
    #     if url_args:
    #         uri = self._routes[route].format(**url_args)
    #     else:
    #         uri = self._routes[route]

    #     url = urljoin(self.root, uri)

    #     # Custom headers
    #     headers = {
    #         "X-Kite-Version": self.kite_header_version,
    #         "User-Agent": self._user_agent()
    #     }

    #     if self.api_key and self.access_token:
    #         # set authorization header
    #         auth_header = self.api_key + ":" + self.access_token
    #         headers["Authorization"] = "token {}".format(auth_header)

        
    #     print("Request: {method} {url} {params} {headers}".format(method=method, url=url, params=params, headers=headers))

    #     # prepare url query params
    #     if method in ["GET", "DELETE"]:
    #         query_params = params

    #     try:
    #         r = self.reqsession.request(method,
    #                                     url,
    #                                     json=params if (method in ["POST", "PUT"] and is_json) else None,
    #                                     data=params if (method in ["POST", "PUT"] and not is_json) else None,
    #                                     params=query_params,
    #                                     headers=headers,
    #                                     verify=not self.disable_ssl,
    #                                     allow_redirects=True,
    #                                     timeout=self.timeout,
    #                                     proxies=self.proxies)
    #     # Any requests lib related exceptions are raised here - https://requests.readthedocs.io/en/latest/api/#exceptions
    #     except Exception as e:
    #         raise e

    #     print("Response: {code} {content}".format(code=r.status_code, content=r.content))

    #     # Validate the content type.
    #     if "json" in r.headers["content-type"]:
    #         try:
    #             data = r.json()
    #         except ValueError:
    #             raise ex.DataException("Couldn't parse the JSON response received from the server: {content}".format(
    #                 content=r.content))

    #         # api error
    #         if data.get("status") == "error" or data.get("error_type"):
    #             # Call session hook if its registered and TokenException is raised
    #             if self.session_expiry_hook and r.status_code == 403 and data["error_type"] == "TokenException":
    #                 self.session_expiry_hook()

    #             # native Kite errors
    #             exp = getattr(ex, data.get("error_type"), ex.GeneralException)
    #             raise exp(data["message"], code=r.status_code)

    #         return data["data"]
    #     elif "csv" in r.headers["content-type"]:
    #         return r.content
    #     else:
    #         raise ex.DataException("Unknown Content-Type ({content_type}) with response: ({content})".format(
    #             content_type=r.headers["content-type"],
    #             content=r.content))

    # def getCurrentLTP(self, scripCode):
    #     try:
    #         return self.kite.ltp(scripCode)[scripCode]['last_price']
    #     except Exception as e:
    #         print(f"Error getting LTP {scripCode}")
    #         return -1
    
    # def getLTPMulti(self, scrip_list):
    #     return self.kite.ltp(scrip_list)
    
    # def getCurrentDateTime(self):
    #     return datetime.datetime.now()
    
    # def getCurrentDate(self):
    #     tdate = datetime.datetime.today().date()
    #     print('getCurrentDate', tdate)
    #     return tdate

    # def getCurrentTime(self):
    #     CurrentDateTime = datetime.datetime.now()
    #     CurrentTime = datetime.time(CurrentDateTime.hour, CurrentDateTime.minute, CurrentDateTime.second)
    #     return CurrentTime

    # def getinstruments(self, exchange):
    #     if exchange == 'NSE':
    #         return self.kite.instruments(exchange=self.kite.EXCHANGE_NSE)
    #     else:
    #         return self.kite.instruments(exchange=self.kite.EXCHANGE_NFO)
    
