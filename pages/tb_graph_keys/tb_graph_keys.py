from os import path, mkdir
from lib.mCommon.thingsboard import *
import pandas as pd
import matplotlib.pyplot as plt
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
        if long_id not in SETTINGS["BlacklistNodes"]:
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
    keys_list = SETTINGS['KeysList']
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

    # Plot each key
    for k in keys_list:
        fig, ax = plt.subplots(figsize=(12, 6))
        for dev_eui in my_devices:
            try:
                this_dev_k = df_dict[dev_eui].copy()
                this_dev_k = this_dev_k.dropna(subset=[k])
                plt.plot(this_dev_k.index, this_dev_k.loc[:, k], label=dev_eui)
            except KeyError:
                logger.debug("Key Error for %s" % dev_eui)
        plt.xlabel("Timestamp")
        plt.ylabel(k)
        plt.title(k+" Graph")

        plt.legend()
        plt.show()
