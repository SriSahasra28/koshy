from celery import Celery
import mysql.connector as sqlConnector
import logging
from background.set import settings
setting = settings()
set = setting.get_db()
#logging.basicConfig(level=logging.DEBUG)

app = Celery(
    'myapp',
    broker='amqp://localhost', 
    backend='rpc://',
    CELERYD_HIJACK_ROOT_LOGGER=False
)

DB_CONFIG = {
    'host': set[2],
    'port': set[3],
    'user': set[0],
    'password': set[1],
    'database': set[4]
}
@app.task
def batch_insert_trade_logs(batch_data):
    """
    Insert batch data into the database using the stored procedure `InsertTradeLog`.

    Args:
        batch_data (list of tuples): List of tuples where each tuple contains data for the procedure.
    """
    try:
        con = sqlConnector.connect(**DB_CONFIG)
        cursor = con.cursor()
        procedure_call = "CALL InsertTradeLog(%s, %s, %s, %s, %s, %s)"

        for data in batch_data:
            cursor.execute(procedure_call, data)
        
        con.commit()
        
    except Exception as e:
        logging.error(f"Error inserting trade logs: {e}")
        raise e

    finally:
        cursor.close()
        con.close()

@app.task
def insert_one_min_ohlc_proc_batch(batch_data):
    """
    Insert batch data into the database using the stored procedure `Insert_one_min_ohlc`.

    Args:
        batch_data (list of tuples): List of tuples where each tuple contains data for the procedure.
    """
    try:
        con = sqlConnector.connect(**DB_CONFIG)
        cursor = con.cursor()
        procedure_call = "CALL Insert_one_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        for data in batch_data:
            cursor.execute(procedure_call, data)
        
        con.commit()
        
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise e

    finally:
        cursor.close()
        con.close()

@app.task
def Insert_three_min_ohlc_proc_batch(batch_data):
    try:
        con = sqlConnector.connect(**DB_CONFIG)
        cursor = con.cursor()
        procedure_call = "CALL Insert_three_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for data in batch_data:
            cursor.execute(procedure_call, data)
        con.commit()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise e
    finally:
        cursor.close()
        con.close()

@app.task
def Insert_two_min_ohlc_proc_batch(batch_data):
    try:
        con = sqlConnector.connect(**DB_CONFIG)
        cursor = con.cursor()
        procedure_call = "CALL Insert_two_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for data in batch_data:
            cursor.execute(procedure_call, data)
        con.commit()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise e
    finally:
        cursor.close()
        con.close()

@app.task
def Insert_five_min_ohlc_proc_batch(batch_data):
    try:
        con = sqlConnector.connect(**DB_CONFIG)
        cursor = con.cursor()
        procedure_call = "CALL Insert_five_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for data in batch_data:
            cursor.execute(procedure_call, data)
        con.commit()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise e
    finally:
        cursor.close()
        con.close()

@app.task
def Insert_ten_min_ohlc_proc_batch(batch_data):
    try:
        con = sqlConnector.connect(**DB_CONFIG)
        cursor = con.cursor()
        procedure_call = "CALL Insert_ten_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for data in batch_data:
            cursor.execute(procedure_call, data)
        con.commit()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise e
    finally:
        cursor.close()
        con.close()

@app.task
def Insert_fifteen_min_ohlc_proc_batch(batch_data):
    try:
        con = sqlConnector.connect(**DB_CONFIG)
        cursor = con.cursor()
        procedure_call = "CALL Insert_fifteen_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for data in batch_data:
            cursor.execute(procedure_call, data)
        con.commit()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise e
    finally:
        cursor.close()
        con.close()

@app.task
def Insert_thirty_min_ohlc_proc_batch(batch_data):
    try:
        con = sqlConnector.connect(**DB_CONFIG)
        cursor = con.cursor()
        procedure_call = "CALL Insert_thirty_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for data in batch_data:
            cursor.execute(procedure_call, data)
        con.commit()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise e
    finally:
        cursor.close()
        con.close()

@app.task
def Insert_hour_ohlc_proc_batch(batch_data):
    try:
        con = sqlConnector.connect(**DB_CONFIG)
        cursor = con.cursor()
        procedure_call = "CALL Insert_hour_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for data in batch_data:
            cursor.execute(procedure_call, data)
        con.commit()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise e
    finally:
        cursor.close()
        con.close()


if __name__ == '__main__':
    app.start()
