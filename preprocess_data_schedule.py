import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import re
pd.set_option('display.max_rows', 100)
from datetime import datetime, timedelta

class preprocess_datetime:
    def __init__(self, raw_data:pd.core.frame.DataFrame):
        """Inisiasi data"""
        column_prerequisites = ['origin_time','destination_time', 'tanggal','jurusan_kode','load_factor']
        columns_existence = all(col in raw_data.columns for col in column_prerequisites)
        if columns_existence:
            print("Semua kolom persyaratan terdapat di data. Silakan lanjutkan.")
        else:
            missing_columns = [col for col in column_prerequisites if col not in raw_data.columns]
            raise ValueError(f'Tolong lengkapi data dengan kolom: {missing_columns} ;agar dapat melanjutkan operasi.')
        return

    def str_datetime(self,x:str):
        """Untuk menyesuaikan time agar bisa diformat datetime dalam python."""
        while len(x)<6:
            x = "0"+x
        result = x[:-4]+":"+x[-4:-2]+":"+x[-2:]
        return(result)
    
    def round_up_time(self,time_str):
        """Untuk membulatkan ke atas waktu peejam"""
        hours, minutes, _ = map(int, time_str.split(':'))
        if minutes > 0:
            hours += 1
        return f'{hours:02d}:00:00'
    
    def strip_datetime(self, data):
        """Untuk strip time agar bisa dibuat dalam format datetime"""
        data['origin_time'] = data['origin_time'].astype(int).astype(str)
        data['origin_time'] = data['origin_time'].apply(self.str_datetime)
        data['origin_time'] = data['origin_time'].apply(lambda x: x.replace("24","00"))
        data['origin_time'] = data['origin_time'].str.strip()

        data['destination_time'] = data['destination_time'].astype(int).astype(str)
        data['destination_time'] = data['destination_time'].apply(self.str_datetime)
        data['destination_time'] = data['destination_time'].apply(lambda x: x.replace("24","00"))
        data['destination_time'] = data['destination_time'].str.strip()
        return data
    
    def fix_datetime_row(self, row):
        hour = row['h_des']
        minutes = row['m_des']
        second = row['s_des']
        if second >= 60:
            minutes += 1
            second -= 60
        if minutes >= 60:
            hour += 1
            minutes -= 60
        return pd.Series({'h_des': hour, 'm_des': minutes, 's_des': second})
    
    def aug_time(self,x:str):
        """Untuk setiap format time yang belum format 'xx:xx:xx' akan dilakukan perluasan elemen."""
        while len(x)<2:
            x = '0'+ x
        return x

    def adjust_datetime(self, row):
        date_str = row['tanggal']
        time_str = row['destinatime']

        # Split date and time components
        date_components = [int(x) for x in date_str.split('-')]
        time_components = [int(x) for x in time_str.split(':')]

        # Create timedelta for the time component
        time_delta = timedelta(hours=time_components[0], minutes=time_components[1], seconds=time_components[2])

        # Create datetime object
        datetime_obj = datetime(*date_components) + time_delta
        return datetime_obj

    def set_datetime(self,data):
        """Dari 'pecahan' time, kita gabungkan agar datetime"""
        data['origin_datetime'] = pd.to_datetime(data['tanggal']+' '+data['origin_time'])

        data[['h_des','m_des','s_des']] =data['destination_time'].str.split(':',expand=True)
        data['h_des'] = data['h_des'].astype(int)
        data['m_des'] = data['m_des'].astype(int)
        data['s_des'] = data['s_des'].astype(int)
        data[['h_des', 'm_des', 's_des']] = data.apply(self.fix_datetime_row, axis=1)
        data['h_des'] = data['h_des'].astype(str).apply(self.aug_time)
        data['m_des'] = data['m_des'].astype(str).apply(self.aug_time)
        data['s_des'] = data['s_des'].astype(str).apply(self.aug_time)
        data['destinatime'] = data['h_des']+':' + data['m_des'] +":"+ data['s_des']
        data['destination_datetime'] = data.apply(self.adjust_datetime,axis=1)
        data.drop(columns=['h_des','m_des','s_des','destinatime'],inplace=True)
        return data

    
    def delete_unusable_shuttle(self,data):
        data['weekofyear']=data['origin_datetime'].dt.isocalendar().week

        """Semua fleet tak terpakai ditandai dengan "SEWA" pada kode rute"""
        data = data[~data['jurusan_kode'].str.contains('SEWA',case=False)]
        data = data[~(data['load_factor']>100)]
        return data

    def Adjust_Datetime(self,data, save=True):
        data = self.strip_datetime(data)
        data = self.set_datetime(data)
        data = self.delete_unusable_shuttle(data)
        """Membulatkan ke atas waktu keberangkatan perjam"""
        data['origin_period'] = data['origin_time'].apply(self.round_up_time)
        if save == True:
            data.to_csv('data_loadfactor.csv',index=False)
            print('Selamat, data Anda yang telah dipreprocess sekarang telah tersimpan di direktori yang sama dengan nama: "data_loadfactor.csv"')
        return data

