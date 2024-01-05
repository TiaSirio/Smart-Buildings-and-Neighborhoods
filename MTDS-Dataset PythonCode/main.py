import pandas as pd
import datetime as dt
import random as rd
import time
import os
import xlsxwriter
import numpy as np

fake_location = ["A", "B", "C", "D", "E"]
fake_location_n = ["S", "0", "1", "2", "3"]


def write_files(df, path):
    df.to_csv(path + "dataset.csv", sep=";", decimal=".", index=False)
    df.to_excel(path + 'dataset.xlsx', engine='xlsxwriter', index=False)


def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(time_format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, '%d/%m/%Y %H:%M', prop)


def generate_fake_data(n):
    temperature = []
    humidity = []
    date_time = []
    sensor_position = []

    for i in range(n):
        temperature.append(round(rd.uniform(10.02, 37.50), 2))
        humidity.append(round(rd.uniform(0, 100), 2))
        d = random_date("01/04/2020 00:00", "31/08/2022 23:59", rd.random())
        date_time.append(d)
        sensor_position.append(fake_location[rd.randint(0, 4)]+"."+fake_location[rd.randint(0, 4)]+"."+fake_location_n[rd.randint(0, 4)]+"."+fake_location[rd.randint(0, 4)])
    return temperature, humidity, date_time, sensor_position


print("Starting...")

temp, hum, datetime, location = generate_fake_data(100000)

df = pd.DataFrame([])
df['Temperature'] = temp
df["Humidity"] = hum
df["Location"] = location
df["Date Time"] = datetime
df["Date Time"] = pd.to_datetime(df["Date Time"], format="%d/%m/%Y %H:%M")
df = df.groupby(["Location", "Date Time"]).agg({"Temperature": "mean", "Humidity": "mean"}).reset_index()
df = df.sort_values(['Date Time'])

path = "../DataOut/"
isExist = os.path.exists(path)
if not isExist:
    os.makedirs(path)

write_files(df, path)

print("End")
