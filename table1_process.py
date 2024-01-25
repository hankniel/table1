from glob import glob
from operator import itemgetter
import json
import pandas as pd
import numpy as np
from functools import reduce
from datetime import datetime, timedelta
import os
import re

def convert_str_to_date(raw_date):
    new_date = datetime.strptime(raw_date, '%d/%m/%Y %H:%M:%S')
    return new_date

def convert_pandas_date(raw_date):
    new_date = datetime.strptime(raw_date, '%Y-%m-%d %H:%M:%S')
    return new_date

files = [f for f in os.listdir('vasp') if re.match(r'vasp\.2023\-1[10]', f)] #Matching for vasp files in t10 and t11

def get_all_log():
    ds_goi_cuoc = ['C120']
    time_threshold = timedelta(days=29)
    fz = open('het_han_hoac_huy_goi_C120_t11.txt', encoding='utf-8', mode='w+')
    ft = open('chuyen_goi_t11.txt', encoding='utf-8', mode='w+')
    for f_name in files:
        count_het_han = 0
        count_dang_ky_moi = 0
        print(f_name)
        for line in open(f_name, encoding='utf-8', mode='r'):
            if 'DELELE' in line:                #Nếu log type là DELETE (DELELE)
                line_split = line.split(',')
                service_name = line_split[5]
                if service_name in ds_goi_cuoc:     #Và thuộc các loại gói cước trên thì ghi vào file hết hạn
                    fz.write(line)
                    count_het_han += 1  
            elif 'REGISTER' in line:            #Nếu log type là REGISTER thì đó là chuyển gói
                line_split = line.split(',')
                service_name = line_split[5]
                str_regis_datetime = line_split[7]
                str_expire_datetime = line_split[8]
                int_price = int(line_split[6])
                if convert_str_to_date(str_expire_datetime) - convert_str_to_date(str_regis_datetime) > time_threshold  \
                    and int_price > 0\
                        and service_name not in ds_goi_cuoc:
                    ft.write(line)
                    count_gia_han += 1
        print(count_gia_han)
        print(count_dang_ky_moi)
    fz.close()
    ft.close()

def convert_log():
    for f_name in glob('*.txt'):
        print(f_name)
        new_f_name = f_name.replace('.txt', '.json')
        fz = open(new_f_name, encoding='utf-8', mode='w+')
        for line in open(f_name, encoding='utf-8', mode='r'):
            line_split = line.strip().split(',')
            type_log = line_split[0]
            isdn = line_split[2]
            service_code = line_split[3]
            group_code = line_split[4]
            service_name = line_split[5]
            service_price = int(line_split[6])
            str_regis_datetime = line_split[7]
            str_expire_datetime = line_split[8]
            str_end_datetime = line_split[9] if line_split[9] != '""' else None
            day_key = str_regis_datetime.split(' ')[0].replace('/', '_')
            reg_code = line_split[10]
            end_code = line_split[11] if line_split[11] != '""' else None
            extend_num = line_split[12]
            regis_datetime = str(convert_str_to_date(str_regis_datetime))
            expire_datetime = str(convert_str_to_date(str_expire_datetime))
            end_datetime = str(convert_str_to_date(str_end_datetime)) if str_end_datetime else None
            log_dict = {'type_log': type_log, 'day_key': day_key, 'isdn': isdn, 'service_code': service_code,
                        'service_name': service_name, 'service_price': service_price,
                        'regis_datetime': regis_datetime, 'group_code': group_code,
                        'expire_datetime': expire_datetime, 'end_datetime': end_datetime,
                        'reg_code': reg_code,
                        'end_code': end_code, 'extend_num': extend_num}
            fz.write(json.dumps(log_dict, ensure_ascii=False))
            fz.write('\n')
        fz.close()
        
def mapping_data():
    het_han = pd.read_json('het_han_hoac_huy_goi_C120_t11.json', lines=True, dtype={'isdn': object})
    chuyen_goi = pd.read_json('chuyen_goi_t11.json', lines=True, dtype={'isdn': object})
    dfb = [het_han, chuyen_goi]
    done_mapping_table = reduce(lambda left, right: pd.merge(left, right, on='isdn', how='left', suffixes=('_het_han', '_chuyen_goi')), dfb)
    done_mapping_table = done_mapping_table.replace({np.nan: None})
    done_mapping_table = done_mapping_table.where(pd.notnull(done_mapping_table), None)
    done_mapping_table = done_mapping_table.drop_duplicates()
    z = done_mapping_table.to_dict(orient='records')
    fz = open('full.json', encoding='utf-8', mode='w+')
    for i in z:
        fz.write(json.dumps(i, ensure_ascii=False))
        fz.write('\n')
    fz.close()
    
def export_data_log():
    fz = open('chuyen_tu_goi_C120_sang_goi_khac_t11.json', encoding='utf-8', mode='w+')
    for line in open('full.json', encoding='utf-8', mode='r'):
        a = json.loads(line)
        isdn = str(a['isdn'])
        service_name_x = a['service_name_het_han']
        expired_service = a['service_name_het_han']
        expired_service_price = a['service_price_het_han']
        expired_service_regis_date = a['regis_datetime_het_han']
        expired_service_expired_date = a['expire_datetime_het_han']
        regis_service_expired_date = a['expire_datetime_chuyen_goi']
        regis_service = a['service_name_chuyen_goi']
        regis_service_price = a['service_price_chuyen_goi']
        expired_service_end_date = a['end_datetime_het_han']
        regis_service_regis_date = a['regis_datetime_chuyen_goi']
        service_name_y = a['service_name_chuyen_goi']
        data_dict = {'isdn': isdn, 'expired_service': expired_service, 'expired_service_price': expired_service_price,
                     'expired_service_regis_date': expired_service_regis_date,
                     'expired_service_expired_date': expired_service_expired_date,
                     'expired_service_end_date': expired_service_end_date,
                     'regis_service': regis_service, 'regis_service_price': regis_service_price,
                     'regis_service_regis_date': regis_service_regis_date,
                     'regis_service_expired_date': regis_service_expired_date}
        if service_name_x:
            if service_name_y:
                if convert_pandas_date(regis_service_regis_date) > convert_pandas_date(expired_service_end_date):
                    data_dict['log_type'] = 'chuyen_goi'
                    fz.write(json.dumps(data_dict, ensure_ascii=False))
                    fz.write('\n')
    fz.close()
    
def sort_by_time():
    my_list = []
    for line in open('chuyen_tu_goi_C120_sang_goi_khac_t11.json', encoding='utf-8', mode='r'):
        a = json.loads(line)
        my_list.append(a)
    mylist = sorted(my_list, key=itemgetter('isdn', 'regis_service', 'regis_service_regis_date'))
    fz = open('chuyen_tu_goi_C120_sang_goi_khac_t11_sorted.json', encoding='utf-8', mode='w+')
    for i in mylist:
        fz.write(json.dumps(i, ensure_ascii=False))
        fz.write('\n')
    fz.close()
    
def remove_duplicate():
    data = pd.read_json("chuyen_tu_goi_C120_sang_goi_khac_t11.json", lines=True, dtype={'isdn': object})
    count_isdn = data['isdn'].value_counts().reset_index()
    isdn_list = count_isdn[count_isdn['count']>1]['isdn']
    table1_csv = data[data['isdn'].isin(isdn_list)==False]
    table1_csv.to_csv("table1.csv")