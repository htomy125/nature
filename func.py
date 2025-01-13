#!/usr/local/bin/python
from uuid6 import uuid7
from remo import NatureRemoAPI
import oracledb
import datetime
import pprint
import configs

device_name = configs.device_name
api = NatureRemoAPI(configs.apikey)

def get_nature_events() :
    events = {}
    events["probe_time"] = datetime.datetime.now(datetime.timezone.utc)

    devices = api.get_devices()
    for d in devices :
        if d.name == device_name :
            events["hu"] = d.newest_events["hu"]
            events["te"] = d.newest_events["te"]
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
            ids = get_data_ids(["hu", "te"])

            for val_type in value_types :
                cursor.execute(sql, 
                            [ids[val_type], events["probe_time"], val_type, 
                                events[val_type].val, events[val_type].created_at
                            ]
                        )
        connection.commit()

# main

insert_data(get_nature_events())
