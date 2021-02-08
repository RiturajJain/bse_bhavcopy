"""
This module defines the function for a cron job to download equity bhavcopy zip from https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx every day at 6 PM for yesterday
"""
import codecs
import csv
from datetime import datetime, timedelta
import io
import requests
from zipfile import ZipFile
import redis
from django.conf import settings

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'

# User Agent header is required as bseindia.com
# returns 403 error when downloading csv data
headers = {'User-Agent': user_agent}

required_fields = ('SC_CODE', 'SC_NAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE')

redis_instance = redis.StrictRedis(host=settings.REDIS_HOST,
                                   port=settings.REDIS_PORT, db=0)


def download_bhavcopy():
    """
    This function runs as a cron.
    It downloads the equity data from yesterday and writes it to the redis db.
    """
    yesterday_date = (datetime.today() - timedelta(days=1)).strftime('%d%m%y')
    response = requests.get(
        f'https://www.bseindia.com/download/BhavCopy/Equity/EQ{yesterday_date}_CSV.ZIP', headers=headers
    )

    # If the data for yesterday is not found (due to weekend or holiday)
    if response.status_code == 404:
        return

    with ZipFile(io.BytesIO(response.content)) as zip_file:
        zip_info = zip_file.infolist()[0]
        with zip_file.open(zip_info) as file:
            data = csv.DictReader(codecs.iterdecode(file, 'utf-8'))
            # Remove the old data
            redis_instance.flushdb()
            for row in data:
                name = row['SC_NAME'].strip().lower()
                redis_instance.hset(name, mapping={
                    field: row[field].strip() for field in required_fields
                })
