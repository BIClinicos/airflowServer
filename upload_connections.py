
# servidor ms sql:
# conn_id : DatamilesWinSQL
# host : 70.37.62.1
# Passw: Y0n4t4n2021airflow
# Usauario: iaaplicada

from airflow import settings
from airflow.models import Connection
conn = Connection(
        conn_id='DatamilesWinSQL',
        conn_type='mssql',
        host='70.37.62.1',
        login='iaaplicada',
        password='Y0n4t4n2021airflow',
        port=1433
) #create a connection object
session = settings.Session() # get the session
session.add(conn)
session.commit()


conn = Connection(
        conn_id='blobqphanalytics',
        conn_type='mssql',
        host='70.37.62.1',
        login='blobstorageqphanalytics',
        password='+NUHr5FhEkvt9IVruKxFuTGRaqm943LWH/t+zV/cC7hs8g/+FutEpOA0+Prm87YOGefrx+m2/yfStBWaYr9Qpg==',
        port=1433
) #create a connection object
session = settings.Session() # get the session
session.add(conn)
session.commit()