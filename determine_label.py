import numpy as np
import pandas as pd
pd.set_option('display.max_rows', 100)
import re
from datetime import datetime

class determine_label_shuttle:
    def __init__(self) -> None:
        return
    
    def naming_pool(self,data_schedule, data_branch):
        data_main = pd.merge(left=data_schedule,
                        right=data_branch,
                        how='left',
                        left_on='origin',
                        right_on='kode',)
        data_main.drop(columns=['alamat','kota','maps','latitude','longitude','deleted','kode'],inplace=True)
        data_main.rename(columns={'nama':'origin_name'},inplace=True)
        data_main = pd.merge(left=data_main,
                        right=data_branch,
                        how='left',
                        left_on='destination',
                        right_on='kode',)
        data_main.drop(columns=['alamat','kota','maps','latitude','longitude','deleted','kode'],inplace=True)
        data_main.rename(columns={'nama':'destination_name'},inplace=True)
        return data_main
    
    def filter_regular_holiday(self, data_main):
        data_main['main_key'] = data_main['origin'] + '-' + data_main['destination']
        data_main['origin_datetime'] = pd.to_datetime(data_main['origin_datetime'])
        data_main['destination_datetime'] = pd.to_datetime(data_main['destination_datetime'])
        data_main['origin_period'].replace({'24:00:00':"00:00:00"},inplace=True)
        data_main['origin_period'] = pd.to_datetime(data_main['origin_period'])
        condition = data_main['destination_datetime']<data_main['origin_datetime']
        data_main.loc[condition,"destination_datetime"] = data_main.loc[condition,"destination_datetime"]+ pd.Timedelta(days=1)
        data_main['duration'] = data_main['destination_datetime']-data_main['origin_datetime']
        data_main['dayofweek'] = data_main['origin_datetime'].dt.day_name()

        start_date_lebaran = pd.to_datetime('2023-04-15')
        end_date_lebaran = pd.to_datetime('2023-04-30')
        start_date_nataru = pd.to_datetime('2023-12-18')
        end_date_nataru = pd.to_datetime('2023-12-31')
        mask1 = (data_main['origin_datetime'].between(start_date_lebaran, end_date_lebaran,inclusive='both'))
        mask2 = (data_main['origin_datetime'].between(start_date_nataru, end_date_nataru,inclusive='both'))
        complement_mask = ~(mask1 | mask2)
        union_mask = (mask1 | mask2)
        df_regular = data_main[complement_mask]
        df_nataru = data_main[mask2]
        df_lebaran = data_main[mask1]
        df_holiday = data_main[union_mask]
        return (df_regular, df_holiday, df_nataru, df_lebaran)
    
    def durasi_per_rute(self, data_schedule, data_branch):
        data_branch = data_schedule[['origin','destination','main_key','origin_name','destination_name']].sort_values(by=['origin','destination']).copy()
        data_branch.drop_duplicates(inplace=True)

        data_durasi_per_rute = data_schedule.groupby(['main_key'])[['duration']].median().reset_index()
        return data_durasi_per_rute
    
    def hour_of_week(self,data_schedule):
        data_hour_of_week = data_schedule.groupby(['main_key','dayofweek','origin_period','origin','origin_name','destination_name','destination','armada_kode'], as_index=False).agg({
            'load_factor' : 'mean'
        })
        return data_hour_of_week
    
    def rute_terbaik_setiap_shuttle(self, data_durasi_per_rute, data_hour_of_week):
        """Merge Data supaya dapet bahan untuk jadwal sementara"""
        data_prejadwal = pd.merge(data_hour_of_week,data_durasi_per_rute,on='main_key',how='left')
        data_prejadwal['arrival_time'] = data_prejadwal['origin_period']+data_prejadwal['duration']
        data_prejadwal = data_prejadwal[['main_key', 'dayofweek','armada_kode', 'origin_period', 'origin','origin_name', 'duration', 'arrival_time','destination','destination_name','load_factor']]

        """Cek jika arrival timenya beda di besoknya atau engga"""
        data_prejadwal['tomorrow?']=data_prejadwal['arrival_time'].dt.date - data_prejadwal['origin_period'].dt.date
        data_prejadwal['tomorrow?'].replace({'0:00:00':0,'1 day, 0:00:00':1},inplace=True)

        """Menentukan Rute mana yang konsisten dilalui shuttle dengan cara melihat history rute pp yang paling sering jalan"""
        dfpreja = data_prejadwal.copy()
        df_uod = pd.DataFrame(columns=data_prejadwal.columns)
        non_eligible_shuttle = []

        for i,armada in enumerate(data_prejadwal['armada_kode'].unique()):
            # print(f'i={i}\narmada={armada}')
            df_vc = dfpreja.loc[dfpreja['armada_kode']==armada,['origin','destination']].value_counts().reset_index()
            mask = df_vc.apply(lambda row: row['origin'] in df_vc['destination'].values and row['destination'] in df_vc['origin'].values, axis=1)
            df_vc = df_vc[mask].reset_index(drop=True)
            if not df_vc.empty:  # Check if df_vc is not empty before accessing elements
                highest_origin = df_vc['origin'][0]
                highest_destination = df_vc['destination'][0]
                # print(f'highest_origin:{highest_origin}\nhighest_destination:{highest_destination}')
                df_od = dfpreja[((dfpreja['armada_kode']==armada) & (dfpreja['origin'] == highest_origin) & (dfpreja['destination'] == highest_destination)) |
                    ((dfpreja['armada_kode']==armada) & (dfpreja['origin'] == highest_destination) & (dfpreja['destination'] == highest_origin))].reset_index(drop=True)
                df_uod = pd.concat([df_uod,df_od])
            else:
                non_eligible_shuttle.append(armada)
            # print(f"df_uod's shape:{df_uod.shape}\n")
            df_uod = df_uod.reset_index(drop=True)
            df_vc.drop(df_vc.index, inplace=True)
        data_prejadwal = df_uod

        return data_prejadwal

    def generate_label(self,data_schedule, data_branch, tipe=['Regular', 'Holiday','Nataru', 'Lebaran'],save=True):
        data_main = self.naming_pool(data_schedule,data_branch)
        (data_regular, data_holiday, data_nataru, data_lebaran) = self.filter_regular_holiday(data_main)
        if tipe == 'Regular':
            data_processed = data_regular
        elif tipe =='Holiday':
            data_processed = data_holiday
        elif tipe == 'Nataru':
            data_processed = data_nataru
        elif tipe == 'Lebaran':
            data_processed = data_lebaran
        else:
            raise ValueError(f'Parameter "tipe" salah. Silakan masukan diantara keempat tipe ini: ["Regular", "Holiday","Nataru", "Lebaran"]')
        data_durasi_per_rute = self.durasi_per_rute(data_processed, data_branch)
        data_hour_of_week = self.hour_of_week(data_processed)
        data_prejadwal = self.rute_terbaik_setiap_shuttle(data_durasi_per_rute,data_hour_of_week)
        data_prejadwal['arrival_time'] = data_prejadwal['arrival_time'].apply(lambda x: datetime.combine(datetime.today().date(), x.time()))
        if save == True:
            data_prejadwal.to_csv('Shuttle.csv',index=False)
            print('Selamat, data Anda untuk penjadwalan shuttle sekarang telah tersimpan di direktori yang sama dengan nama: "Prejadwal_Shuttle.csv"')
        return data_prejadwal
    
class determine_label_pool:
    def __init__(self) -> None:
        return
    
    def naming_pool(self,data_schedule, data_branch):
        data_main = pd.merge(left=data_schedule,
                        right=data_branch,
                        how='left',
                        left_on='origin',
                        right_on='kode',)
        data_main.drop(columns=['alamat','kota','maps','latitude','longitude','deleted','kode'],inplace=True)
        data_main.rename(columns={'nama':'origin_name'},inplace=True)
        data_main = pd.merge(left=data_main,
                        right=data_branch,
                        how='left',
                        left_on='destination',
                        right_on='kode',)
        data_main.drop(columns=['alamat','kota','maps','latitude','longitude','deleted','kode'],inplace=True)
        data_main.rename(columns={'nama':'destination_name'},inplace=True)
        return data_main
    
    def filter_regular_holiday(self, data_main):
        data_main['main_key'] = data_main['origin'] + '-' + data_main['destination']
        data_main['origin_datetime'] = pd.to_datetime(data_main['origin_datetime'])
        data_main['destination_datetime'] = pd.to_datetime(data_main['destination_datetime'])
        data_main['origin_period'].replace({'24:00:00':"00:00:00"},inplace=True)
        data_main['origin_period'] = pd.to_datetime(data_main['origin_period'])
        condition = data_main['destination_datetime']<data_main['origin_datetime']
        data_main.loc[condition,"destination_datetime"] = data_main.loc[condition,"destination_datetime"]+ pd.Timedelta(days=1)
        data_main['duration'] = data_main['destination_datetime']-data_main['origin_datetime']
        data_main['dayofweek'] = data_main['origin_datetime'].dt.day_name()

        start_date_lebaran = pd.to_datetime('2023-04-15')
        end_date_lebaran = pd.to_datetime('2023-04-30')
        start_date_nataru = pd.to_datetime('2023-12-18')
        end_date_nataru = pd.to_datetime('2023-12-31')
        mask1 = (data_main['origin_datetime'].between(start_date_lebaran, end_date_lebaran,inclusive='both'))
        mask2 = (data_main['origin_datetime'].between(start_date_nataru, end_date_nataru,inclusive='both'))
        complement_mask = ~(mask1 | mask2)
        union_mask = (mask1 | mask2)
        df_regular = data_main[complement_mask]
        df_nataru = data_main[mask2]
        df_lebaran = data_main[mask1]
        df_holiday = data_main[union_mask]
        return (df_regular, df_holiday, df_nataru, df_lebaran)
    
    def durasi_per_rute(self, data_schedule, data_branch):
        data_branch = data_schedule[['origin','destination','main_key','origin_name','destination_name']].sort_values(by=['origin','destination']).copy()
        data_branch.drop_duplicates(inplace=True)

        data_durasi_per_rute = data_schedule.groupby(['main_key'])[['duration']].median().reset_index()
        return data_durasi_per_rute
    
    def hour_of_week(self,data_schedule,data_branch):
        data_hour_of_week = data_schedule.groupby(['main_key','dayofweek','origin_period','origin','destination'])[['load_factor']].mean().reset_index()
        data_hour_of_week = self.naming_pool(data_hour_of_week,data_branch)
        return data_hour_of_week
    
    def rute_terbaik_setiap_pool(self, data_durasi_per_rute, data_hour_of_week):
        """Merge Data supaya dapet bahan untuk jadwal sementara"""
        data_prejadwal = pd.merge(data_hour_of_week,data_durasi_per_rute,on='main_key',how='left')
        data_prejadwal['arrival_time'] = data_prejadwal['origin_period']+data_prejadwal['duration']
        data_prejadwal = data_prejadwal[['main_key', 'dayofweek', 'origin_period', 'origin','origin_name', 'duration', 'arrival_time','destination','destination_name','load_factor']]

        """Cek jika arrival timenya beda di besoknya atau engga"""
        data_prejadwal['tomorrow?']=data_prejadwal['arrival_time'].dt.date - data_prejadwal['origin_period'].dt.date
        data_prejadwal['tomorrow?'].replace({'0:00:00':0,'1 day, 0:00:00':1},inplace=True)

        return data_prejadwal

    def generate_label(self,data_schedule, data_branch, tipe=['Regular', 'Holiday','Nataru', 'Lebaran'],save=True):
        data_main = self.naming_pool(data_schedule,data_branch)
        (data_regular, data_holiday, data_nataru, data_lebaran) = self.filter_regular_holiday(data_main)
        if tipe == 'Regular':
            data_processed = data_regular
        elif tipe =='Holiday':
            data_processed = data_holiday
        elif tipe == 'Nataru':
            data_processed = data_nataru
        elif tipe == 'Lebaran':
            data_processed = data_lebaran
        else:
            raise ValueError(f'Parameter "tipe" salah. Silakan masukan diantara keempat tipe ini: ["Regular", "Holiday","Nataru", "Lebaran"]')
        data_durasi_per_rute = self.durasi_per_rute(data_processed, data_branch)
        data_hour_of_week = self.hour_of_week(data_processed,data_branch)
        data_prejadwal = self.rute_terbaik_setiap_pool(data_durasi_per_rute,data_hour_of_week)
        data_prejadwal['arrival_time'] = data_prejadwal['arrival_time'].apply(lambda x: datetime.combine(datetime.today().date(), x.time()))
        if save == True:
            data_prejadwal.to_csv('Prejadwal_Pool.csv',index=False)
            print('Selamat, data Anda untuk penjadwalan pool sekarang telah tersimpan di direktori yang sama dengan nama: "Prejadwal_Pool.csv"')
        return data_prejadwal