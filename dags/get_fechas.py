import os


from datetime import datetime, timedelta
from datetime import date

#get_fechas.py
# python ~/dags/get_fechas.py
#Se halla las fechas de cargue de la data 
##now = datetime.now()
now = datetime(2023, 3, 2,05,40,00)
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')

print("Now",now)
print("last_week",last_week)


