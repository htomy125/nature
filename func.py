#!/usr/local/bin/python
from uuid6 import uuid7
# from remo import NatureRemoAPI
import oracledb
import datetime
import requests
import json
import pprint
import configs

def get_nature_events() :
    events = {}
    events["probe_time"] = datetime.datetime.now(datetime.timezone.utc)

    url = "https://api.nature.global/1/devices"
    h = {
            "Accept": "application/json",
            "Authorization": f"Bearer {configs.apikey}"
        }

    devices = json.loads(requests.get(url=url, headers=h).text)

    for d in devices :
        if d["name"] == configs.device_name :
            ne = d["newest_events"]
            for val_type in ["hu", "te"] :
                events[val_type] = ne[val_type]
                events[val_type]["created_at"] = datetime.datetime.strptime(ne[val_type]["created_at"], "%Y-%m-%dT%H:%M:%SZ")
            return events

def get_data_ids(flags:list) :
    ids = {}
    uuid = str(uuid7())
    for f in flags :
        ids[f] =  uuid + "-" + "{:0>4}".format(f)
    return ids

def get_connect() :
    return oracledb.connect(
            config_dir=configs.db_config_dir,
            user=configs.db_user,
            password=configs.db_password,
            dsn=configs.db_dsn,
            wallet_location=configs.db_wallet_location,
            wallet_password=configs.db_wallet_password
        )
    
def insert_data(events) :
    with get_connect() as connection :
        with connection.cursor() as cursor:
            sql = """insert into WKSP_NATURE.LAPIS_VALS (data_id, probe_time, val_type, value, created_at) 
                    values (:data_id, :probe_time, :val_type, :value, :created_at)"""
        
            value_types = ["hu", "te"]
            ids = get_data_ids(value_types)

            for val_type in value_types :
                cursor.execute(sql, 
                            [ids[val_type], events["probe_time"], val_type, 
                                events[val_type]["val"], events[val_type]["created_at"]
                            ]
                        )
        connection.commit()

# main

insert_data(get_nature_events())

