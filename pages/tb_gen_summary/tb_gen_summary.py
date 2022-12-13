from os import path, mkdir
from lib.mCommon.thingsboard import *
import pandas as pd
import yaml
import pytz
from datetime import datetime
import logging


SCRIPT_PATH = path.dirname(path.realpath(__file__))
SETTINGS_PATH = path.join(SCRIPT_PATH, 'settings.yaml')
with open(SETTINGS_PATH, 'r', encoding='utf-8') as settings:
    SETTINGS = yaml.safe_load(settings)

if SETTINGS["Debug"]:
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.DEBUG,
                        handlers=[logging.StreamHandler()
                                  ])
else:
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO,
                        handlers=[logging.StreamHandler()
                                  ])
logger = logging.getLogger()


def get_node_list_from_startid_and_endid(start_node, end_node):
    start_node_int = int(start_node[4:])
    end_node_int = int(end_node[4:])
    node_list = []

    for short_id in range(start_node_int, end_node_int + 1):
        long_id = "260A" + str(short_id)
        try:
            if long_id not in SETTINGS["BlacklistNodes"]:
                node_list.append(long_id)
        except:
            node_list.append(long_id)

    return node_list


if __name__ == '__main__':
    # Make output directory
    try:
        mkdir(path.join(SCRIPT_PATH, SETTINGS["OutputFolder"]))
    except:
        pass

    # Login to thingsboard
    TB_URL = SETTINGS["Thingsboard"]["Host"] + ":" + str(SETTINGS["Thingsboard"]["Port"])
    tb_client = RestClientPE(base_url=TB_URL)
    tb_client.login(username=SETTINGS["Thingsboard"]["Username"], password=SETTINGS["Thingsboard"]["Password"])

    # Generate device spec dictionary
    if SETTINGS["NodeList"]:
        if type(SETTINGS["NodeList"]) == list:
            my_devices = SETTINGS["NodeList"]
        else:
            my_devices = [SETTINGS["NodeList"]]
    else:
        my_devices = get_node_list_from_startid_and_endid(SETTINGS["StartNode"], SETTINGS["EndNode"])

    device_list = get_devices_by_customer_name_as_dict(tb_client, SETTINGS["Thingsboard"]["CustomerName"])
    my_devices_specs = {k: device_list[k] for k in device_list if k in my_devices}
    logger.debug(my_devices_specs)

    # Generate while loop variables
    keys_list = ['data.N.payload.BV', 'data.B.frame_count']
    start_ts = int(pytz.timezone("America/New_York").localize(
        datetime(year=SETTINGS["StartTimestamp"]["Year"], month=SETTINGS["StartTimestamp"]["Month"],
                 day=SETTINGS["StartTimestamp"]["Day"], hour=0)).timestamp()) * 1000
    df_dict = {}

    # While loop to pull data and fill dicts
    for dev_eui in my_devices:
        logger.info('processing %s' % dev_eui)
        try:
            this_dev_spec = my_devices_specs[dev_eui]
        except KeyError:
            logger.warning("Node id %s not found in thingsboard. Moving on." % dev_eui)
            continue
        this_dev = DeviceId(id=this_dev_spec['id']['id'], entity_type=this_dev_spec['id']['entityType'])

        # Get data into dataframe for each key
        df_dict[dev_eui] = pd.DataFrame()
        for k in keys_list:
            logger.debug('processing key %s' % k)
            data = get_timeseries_by_device(tb_client, this_dev, [k], start_ts=start_ts)

            df_data = pd.DataFrame.from_records(data)
            if df_data.empty:
                logger.debug("Empty dataframe.")
                continue
            df_data['ts'] = pd.to_datetime(df_data['ts'], unit='ms')
            df_data['value'] = df_data['value'].apply(pd.to_numeric)
            df_data.set_index('ts', inplace=True)
            df_data.rename(columns={"value": k}, inplace=True)

            df_dict[dev_eui] = pd.concat([df_dict[dev_eui], df_data], axis=1)
            logger.debug('%s: %d' % (k, len(data)))

    # Generate summary
    cols = ['First BV', 'First BV TS', 'Last BV', 'Last Framecount', 'Last Framecount TS', "Duration (seconds)", "Duration (hours)"]
    table = []
    for dev_eui in my_devices:
        row = []

        df_dict[dev_eui]["timestamp"] = pd.to_datetime(df_dict[dev_eui].index)

        zero_fc = df_dict[dev_eui].copy()
        zero_fc = zero_fc[zero_fc['data.B.frame_count'] == 1]
        if zero_fc.empty:
            logger.debug("couldn't find start framecount")
            start_time = 0
        else:
            start_time = int((datetime.strptime(str(zero_fc.index[-1]), "%Y-%m-%d %H:%M:%S") - datetime(1970, 1, 1)).total_seconds())
            start_time_index = zero_fc['timestamp'].iloc[-1]
            df_dict[dev_eui] = df_dict[dev_eui][df_dict[dev_eui]['timestamp'] >= start_time_index]

        no_none_bv = df_dict[dev_eui].copy()
        no_none_bv = no_none_bv.dropna(subset=['data.N.payload.BV'])
        if no_none_bv.empty:
            row.append(None)
            row.append(None)
            row.append(None)
        else:
            row.append(no_none_bv['data.N.payload.BV'].iloc[0])
            row.append(no_none_bv["timestamp"].iloc[0])
            row.append(no_none_bv['data.N.payload.BV'].iloc[-1])

        no_none_fc = df_dict[dev_eui].copy()
        no_none_fc = no_none_fc.dropna(subset=['data.B.frame_count'])
        row.append(no_none_fc['data.B.frame_count'].iloc[-1])
        row.append(no_none_fc.index[-1])
        end_time = int((datetime.strptime(str(no_none_fc.index[-1]), "%Y-%m-%d %H:%M:%S") - datetime(1970, 1, 1)).total_seconds())

        if start_time:
            row.append(end_time-start_time)
            row.append((end_time-start_time)/3600)
        else:
            row.append(None)

        table.append(row)

    summary = pd.DataFrame(table, index=my_devices, columns=cols)
    summary.to_excel(path.join(SCRIPT_PATH, 'output', '%s-%s_%s.xlsx' % (SETTINGS['StartNode'], SETTINGS['EndNode'], start_ts)),
                      index_label='Devices',
                      freeze_panes=(1, 0))
