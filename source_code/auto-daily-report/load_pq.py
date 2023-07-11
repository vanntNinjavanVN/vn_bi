
# Import Libs
import pandas as pd
import numpy as np
import time
import requests
import json
from tenacity import *
import datetime
import pytz
import logging
import pathlib
import os 
pd.set_option('display.max_columns',500)

import warnings
warnings.filterwarnings("ignore")

_path =  pathlib.Path().absolute()
# directory = _path.as_posix()
# print(directory)

raw_token = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vToMKz1a90G9vvE-jF-KCxtxfgjFNlIoiOAsK356fyaC-Kfmkt92hqKsOPvX-txvztXBJlbdP8ClByZ/pub?output=csv')
api_key= raw_token.loc[raw_token['type']=='redash']['context'].values[0]

# Functions to query from redash
@retry(wait=wait_fixed(10), stop=stop_after_attempt(3))
def redash_refresh(query_id, api_key, params={}) -> str:
	"""
	Send POST request to refresh Redash-vn query data
	Use @retry decorator to refresh 3 times; if still error, raise ConnectionError
	
	Input:
	- query_id
	- api_key
	- params
	
	Output: job_id of query
	"""
	_url = f'https://redash-vn.ninjavan.co/api/queries/{query_id}/results'
	_header = {'Authorization': f'Key {api_key}'}
	_body = f'{{"max_age": 0, "parameters": {json.dumps(params, ensure_ascii=False)}}}'
	
	_r = requests.post(url=_url, headers=_header, data=_body.encode('utf-8'))
	
	if not _r.ok:
		raise ConnectionError
	return(_r.json()['job']['id'])

@retry(wait=wait_fixed(10), retry=retry_if_result(lambda x: x is None))
def redash_job_status(job_id, api_key) -> str:
	_url = f'https://redash-vn.ninjavan.co/api/jobs/{job_id}'
	_header = {'Authorization': f'Key {api_key}'}
	
	_r = requests.get(url=_url, headers=_header)
	job_status = _r.json()['job']['status']
	
	if job_status == 3:
		return(_r.json()['job']['query_result_id'])
	elif ((job_status == 1) or (job_status == 2)):
		return(None)
	else:
		raise ConnectionError

def redash_result(result_id, api_key) -> pd.DataFrame:
	"""
	Send GET request to get query result
	
	Input:
	- result_id
	- api_key
	
	Output: dataframe of query result
	"""
	_url = f'https://redash-vn.ninjavan.co/api/query_results/{result_id}'
	_header = {'Authorization': f'Key {api_key}'}
	
	_r = requests.get(url=_url, headers=_header)
	if _r.ok:
		return(pd.DataFrame(_r.json()['query_result']['data']['rows']))
	else:
		raise ConnectionError

@retry(wait=wait_fixed(5), stop = stop_after_attempt(20))
def query(query_id, api_key, params={}) -> pd.DataFrame:
	"""
	Combination of 3 funtions above
	Order of execution: refresh -> check job status -> get result
	
	Input:
	- query_id
	- api_key
	- params
	
	Output: dataframe of query result
	"""
	_jobid = redash_refresh(query_id=query_id, api_key=api_key, params=params)
	print('Query request sent. Waiting for result...')
	
	_resid = redash_job_status(job_id=_jobid, api_key=api_key)
	print('Query completed!')
	
	return(redash_result(result_id=_resid, api_key=api_key))

def fetch_all(sub_query_id, main_query_id, no_of_row, start_date, end_date):
    total_orders = query(sub_query_id, api_key, params = {'start': f'{start_date}', 'end': f'{end_date}' })
    total = pd.to_numeric(total_orders.loc[0, 'total_orders']) 
    print(f'Estimate total orders: {total}')
    estimate_loops = (total//no_of_row) + 1
    print(f'Estimate number of sub-loops: {estimate_loops}')

    df = pd.DataFrame()
    i = 0
    j = 1
    while i <= total and j <= estimate_loops:
        df_query_result = query(main_query_id, api_key, params = {'start': f'{start_date}', 'end': f'{end_date}', 'OFFSET': i, 'no_of_row': no_of_row})
        df = pd.concat([df, df_query_result], sort=False)
        print(f'Number of sub-loop(s) completed: {j}')
        j += 1
        i += no_of_row   
    df.drop_duplicates(inplace=True)

    return df 

logging.basicConfig(filename="/home/vn_bi/source_code/auto-daily-report/load_pq.log",
                    format='%(asctime)s %(message)s',
                    filemode='w',
                    level = logging.INFO
                    )

logger = logging.getLogger()

logger.info('Bắt đầu thiết lập chạy query...')
start = datetime.datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
end = start - datetime.timedelta(days=30)
end1 = start - datetime.timedelta(days=61)
end_month = datetime.date(end1.year, end1.month, 1)

# Run daily Report
query_date_range = pd.date_range(start = end, end = start, freq='D')
query_date_list = []
for query_date in query_date_range:
    query_date_list.append(query_date.strftime('%Y-%m-%d'))

query_date_list = query_date_list[:-1]
query_date_list = tuple(query_date_list) # (oldest_date - newest_date)


logger.info(f'''Bắt đầu chạy query Shippers Daily Report, các đơn tạo từ {end_month.strftime('%Y-%m-%d')} đến {start.strftime('%Y-%m-%d')}...''')
no_of_loop = len(query_date_list)
print(f'Estimate loop: {no_of_loop}')

start_query = time.time()
df = pd.DataFrame()
_step = 1

if __name__ == "__main__":
    try:
        logger.info('Get All Parcels within 3 months for monthly summary...')
        try:
            df_month = fetch_all(sub_query_id = 346, main_query_id = 345, no_of_row = 50000, start_date = end_month.strftime('%Y-%m-%d') , end_date = start.strftime('%Y-%m-%d'))
            time.sleep(3)
        except Exception as e:
            logger.info(e)
            logger.info('Fail to get data!')
            time.sleep(3)

        # Run data daily
        logger.info('Get Parcels Status last 30 days...')
        for _date in query_date_list:
            logger.info(f"{_step}/{no_of_loop}. Bắt đầu query các đơn tạo ngày {_date}")
            _step += 1
            try:
                df_query_result = fetch_all(sub_query_id = 344, main_query_id = 341, no_of_row = 50000, start_date = _date, end_date = _date)
                df = pd.concat([df,df_query_result])  
                time.sleep(3)      
            except Exception as e:
                logger.info(e)
                logger.info(f'Fail to get data date: {_date}')
                time.sleep(3) 

        logger.info(f'Query xong mất {(time.time() - start_query)/60:.0f} phút') 

        time.sleep(3)
        df = df.reset_index(drop=True)
        df_month = df_month.reset_index(drop=True)
        dfs = np.array_split(df, 7)

        folder_name = f"Auto_CX_Data"
        if not os.path.exists(folder_name):
            logger.info('Bắt đầu tạo folder...')
            os.makedirs(folder_name)

        def connect_drive(bi_key):
            #import shipper info from gsheet
            import gspread

            #export report to drive
            from google.oauth2 import service_account
            import gspread
            from pydrive2 import auth, drive
            
            # GDrive setup
            gauth = auth.GoogleAuth()
            scope = ["https://www.googleapis.com/auth/drive"]
            gauth.credentials = auth.ServiceAccountCredentials.from_json_keyfile_dict(bi_key, scope)
            drive = drive.GoogleDrive(gauth)
            gc = gspread.authorize(gauth.credentials)
            return gc, drive

        logger.info('Bắt đầu thiết lập kết nối GDrive...')
        bi_key_retail = {
            "type": "service_account",
            "project_id": "vn-bi-337205",
            "private_key_id": "4790637d92d47d16f8727b9b4a1a247729a87234",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDOXbGagl7UFMPM\ntJBoaZHz3xpvdqcNzfG6EAkuAovSh5mb+9t+TgVEgcNtKfqffUJWwTdXMh3h7Nl5\nt1bvlb9wP3BlJTKHf7cfqZoH9WKTcrWWUnZZCOlDgpdVgDfMdBORp0BVL08b23JS\nQqWoHOaCutr5NScKd6IjfTjihQI4S5RWYjep9QdCpJxB0w9hrBvsTXXWNUSP4wcm\n7y7v0kpcUAE0rTg7qH50SgM1ZuykQhuq40QK2qjdT68TjJqsecp+eZesboSxKG6Z\nnygRkAnz82NUePIe0k4ViWpA1rfyYKqzOaTrHvVwp2ZUiZBgqc8O/jvXOJme+3vi\nSqbEXqDTAgMBAAECggEAMc4pFBpM2rCcROZ7R8fa4tnAnpArZ0iCA57lKEaTCl+R\nsjTJ16Tq3orW6JzOEPoBLcD6I0BEOAeawSZ/g9lHV8bn0HF2zX8Eak7bjvopLV7s\n9ne2emyNJ6736TnFvcFyC6ArBaQiWp3O7I2LP5SiCYEhYi3y2zLeXeRV+02QqJce\nDwP/w50vxOf0KqiI8r97R4XBgkWW4Akf2rgV/wv5zAYivbbTNXmk4t0wfLm0VlWq\nBn6Olgkt+FX8aXjjyhXJKDzgIp97N0Ko7xkc0I6QMQSp3cQd0wx4AjLnis14bkqq\nZ/pGYQcd7hHtjh6zue+Z+Nor3/k3x+rrJasZZSr++QKBgQDvawZkN85bsrjRBNu/\nbYGt6THiT8ryhiXoGd4E+1ZWZL7FlBM+CpG0uR25wfsyhztKzIdFYf9PHzjxeAOg\nmKkhg+ETQl4dXaZzCRo+8sZH3Fa8ccXZntgBJKSdz7CsNm1nOGyBkXwQ0wHov2Db\nG35vj9XRrsQgZdxNv75HA1duJwKBgQDcqKSId22emK6ED/GjQQWbr7zT/WxCoskE\n5f9Ei4YvPKmk1UI5DMWhRrxtBvHu2AeJ1F0cOF/CyrKfpDi6EM+/8ff0cItZL0FN\nxczB/XDPEDOUeeKSwnYhx9mFknUXFXPPiQh/07Zt3NG7BUTnsVRKXidyr2vRhuVq\nG4UfQIMPdQKBgBkZ2G6zZyA4CaZoYv2b5oesNj2q7fUlWV4PMDbHfxLJtzKRxr6v\nbv1KlzI9gy+V/AJolRoSHylVdavjUEYLG1hXMkbJo4JiqivPXYASBfXMxQ82wm6B\nd9YO2G9vMe+sOSYkRUQEU8STytRzFB2EsWnS47D7KLbY1xMTskhRsl3rAoGAc9RK\n2h/m+ROouEYuT8Y4DWuIsbcb9zbAUsPw5ahf/bfYTWBs7MmZjHk3O/wjT38zJdTG\nM6QcEIKalVZ9OJ7OjzGTOUtCusQlgY/NVh+V8fvNN6lrmCJgJIdl1cn1kPJ/4ndK\ncN9Pqgf6SDQR4ZWia49VIcyXylsHXlWn4anZomkCgYAMvlGzr8813rN376BsTQl8\nN5SRrUEEZ9vhOIFi1yeSMBv1Cm0ztFegaEmVdmjAh9al7EZBA0mYKf/q46/VfjLG\nGnQTUk6lvqBHzbgxxjFOHgqqBI51fGPYniUH+cIK/Qg7YaiASkYlzCFAXPbSGIaG\nwjZ8eRISgGf9G0MPJFNYXg==\n-----END PRIVATE KEY-----\n",
            "client_email": "vn-bi-3rd@vn-bi-337205.iam.gserviceaccount.com",
            "client_id": "117767296610410193951",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/vn-bi-3rd%40vn-bi-337205.iam.gserviceaccount.com"
        }

        gc_retail, drive_retail = connect_drive(bi_key_retail)
        print("Done connect to GDrive")

        file_list = drive_retail.ListFile({'q': "'14EJwnFfX8XJUomaeAmQVTruuDpSgW1Lt' in parents and trashed=false"}).GetList()
        logger.info('Delete all files in folder Auto CX Data, folder_id: 14EJwnFfX8XJUomaeAmQVTruuDpSgW1Lt....')
        for file in file_list:
            print(file['title'])
            print(file['id'])
            file.Delete()

        try:
            for _i in range(0,7):
                file_name = f'daily_reports_last_30_days_{_i+1}.pq'
                logger.info(f'{_i+1}/8. {file_name}')
                dfs[_i].to_parquet(f'{folder_name}/{file_name}')
                file_path_df = fr"/home/vn_bi/source_code/auto-daily-report/{folder_name}/{file_name}"
                _shipper_report_df = drive_retail.CreateFile({'parents' : [{'id' : '14EJwnFfX8XJUomaeAmQVTruuDpSgW1Lt'}], 'title' : file_name})
                _shipper_report_df.SetContentFile(file_path_df)
                _shipper_report_df.Upload()  
        except Exception as e:
            logger.info(e)

        try:
            logger.info('8/8. daily_reports_last_3_months.pq')
            df_month.to_parquet(f'{folder_name}/daily_reports_last_3_months.pq')
            file_path_df_month = fr"/home/vn_bi/source_code/auto-daily-report/{folder_name}/daily_reports_last_3_months.pq"
            _shipper_report_df_month = drive_retail.CreateFile({'parents' : [{'id' : '14EJwnFfX8XJUomaeAmQVTruuDpSgW1Lt'}], 'title' : 'daily_reports_last_3_months.pq'})
            _shipper_report_df_month.SetContentFile(file_path_df_month)
            _shipper_report_df_month.Upload()
        except Exception as e:
            logger.info(e)

        logger.info('Done!!')

    except Exception as e:
        logger.info('Fail Run Program')
        logger.info(e)