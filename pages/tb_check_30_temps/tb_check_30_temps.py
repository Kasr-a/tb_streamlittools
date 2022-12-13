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
    if type(SETTINGS["NodeList"]) == list:
        my_devices = SETTINGS["NodeList"]
    else:
        my_devices = [SETTINGS["NodeList"]]

    device_list = get_devices_by_customer_name_as_dict(tb_client, SETTINGS["Thingsboard"]["CustomerName"])
    my_devices_specs = {k: device_list[k] for k in device_list if k in my_devices}
    logger.debug(my_devices_specs)

    # Generate while loop variables
    keys_list = ['data.E.payload.T.1', 'data.E.payload.T.2', 'data.E.payload.T.3',
                 'data.E.payload.T.4', 'data.E.payload.T.5', 'data.E.payload.T.6',
                 'data.E.payload.T.7', 'data.E.payload.T.8', 'data.E.payload.T.9',
                 'data.E.payload.T.10', 'data.E.payload.T.11', 'data.E.payload.T.12',
                 'data.E.payload.T.13', 'data.E.payload.T.14', 'data.E.payload.T.15',
                 'data.E.payload.T.16', 'data.E.payload.T.17', 'data.E.payload.T.18',
                 'data.E.payload.T.19', 'data.E.payload.T.20', 'data.E.payload.T.21',
                 'data.E.payload.T.22', 'data.E.payload.T.23', 'data.E.payload.T.24',
                 'data.E.payload.T.25', 'data.E.payload.T.26', 'data.E.payload.T.27',
                 'data.E.payload.T.28', 'data.E.payload.T.29', 'data.E.payload.T.30'
                 ]
    start_ts = int(pytz.timezone("America/New_York").localize(
        datetime(year=int(SETTINGS["StartTimestamp"]["Year"]),
                 month=int(SETTINGS["StartTimestamp"]["Month"]),
                 day=int(SETTINGS["StartTimestamp"]["Day"]),
                 hour=int(SETTINGS["StartTimestamp"]["Hour"]),
                 minute=int(SETTINGS["StartTimestamp"]["Minute"]))).timestamp()) * 1000
    end_ts = int(pytz.timezone("America/New_York").localize(
        datetime(year=int(SETTINGS["EndTimestamp"]["Year"]),
                 month=int(SETTINGS["EndTimestamp"]["Month"]),
                 day=int(SETTINGS["EndTimestamp"]["Day"]),
                 hour=int(SETTINGS["EndTimestamp"]["Hour"]),
                 minute=int(SETTINGS["EndTimestamp"]["Minute"]))).timestamp()) * 1000

    HIGH_TEMP_THRESHOLD = SETTINGS["HighTempThreshold"]
    LOW_TEMP_THRESHOLD = SETTINGS["LowTempThreshold"]
    FREQUENCY_MS = SETTINGS["FrequencyMS"]

    df_dict = {}
    failure_flags = {}

    # While loop to pull data and fill dicts
    for dev_eui in my_devices:
        failure_flags[dev_eui] = 0
        logger.info('Processing %s ...' % dev_eui)
        try:
            this_dev_spec = my_devices_specs[dev_eui]
        except KeyError:
            logger.warning("Node id %s not found in thingsboard. Moving on." % dev_eui)
            continue
        this_dev = DeviceId(id=this_dev_spec['id']['id'], entity_type=this_dev_spec['id']['entityType'])

        # Get data into dataframe for each key
        df_dict[dev_eui] = pd.DataFrame()
        for e, k in enumerate(keys_list):
            logger.debug('processing key %s' % k)
            data = get_timeseries_by_device(tb_client, this_dev, [k], start_ts=start_ts, end_ts=end_ts)

            df_data = pd.DataFrame.from_records(data)
            if df_data.empty:
                logger.debug("Empty dataframe.")
                continue
            if e == 0:
                df_data['tsr'] = df_data['ts']
            df_data['ts'] = pd.to_datetime(df_data['ts'], unit='ms')
            df_data['value'] = df_data['value'].apply(pd.to_numeric)
            df_data.rename(columns={"value": k}, inplace=True)

            missing_data_list = df_data.loc[df_data[k].isnull(), 'ts'].tolist()
            if missing_data_list:
                failure_flags[dev_eui] = 1
                for missing_data_pt in missing_data_list:
                    logger.info(f"{dev_eui}: Missing {k} at {missing_data_pt}")

            high_temp_list_ts = df_data.loc[df_data[k] > HIGH_TEMP_THRESHOLD, 'ts'].tolist()
            high_temp_list = df_data.loc[df_data[k] > HIGH_TEMP_THRESHOLD, k].tolist()
            if high_temp_list:
                failure_flags[dev_eui] = 1
                for c, high_temp_pt in enumerate(high_temp_list):
                    logger.info(f"{dev_eui}: High temperature found on sensor {k} at {high_temp_list_ts[c]}"
                                f". Temp value: {high_temp_pt}")

            low_temp_list_ts = df_data.loc[df_data[k] < LOW_TEMP_THRESHOLD, 'ts'].tolist()
            low_temp_list = df_data.loc[df_data[k] < LOW_TEMP_THRESHOLD, k].tolist()
            if low_temp_list:
                failure_flags[dev_eui] = 1
                for c, low_temp_pt in enumerate(low_temp_list):
                    logger.info(
                        f"{dev_eui}: Low temperature found on sensor {k} at {low_temp_list_ts[c]}. "
                        f"Temp value: {low_temp_pt}")

            df_data.set_index('ts', inplace=True)

            df_dict[dev_eui] = pd.concat([df_dict[dev_eui], df_data], axis=1)
            logger.debug('%s: %d' % (k, len(data)))


        # Check framecount
        data = get_timeseries_by_device(tb_client, this_dev, ["network.fcnt"], start_ts=start_ts, end_ts=end_ts)

        df_data = pd.DataFrame.from_records(data)
        if df_data.empty:
            logger.debug("Empty dataframe.")
        else:
            df_data['ts'] = pd.to_datetime(df_data['ts'], unit='ms')
            df_data['value'] = df_data['value'].apply(pd.to_numeric)
            df_data.rename(columns={"value": "network.fcnt"}, inplace=True)
            df_data.set_index('ts', inplace=True)
            df_dict[dev_eui] = pd.concat([df_dict[dev_eui], df_data], axis=1)

        # Finalize df
        df_dict[dev_eui].to_excel(path.join(SCRIPT_PATH, 'output',
                                            '%s_%s-%s.xlsx' % (dev_eui, start_ts, end_ts)),
                                  index_label='Timestamp',
                                  freeze_panes=(1, 0))
        df_dict[dev_eui]["DeviceID"] = dev_eui

        # for i in range(1, 6):
        #     logger.info(f"Progress: {i}/5 seconds")
        #     time.sleep(1)

    global_df = pd.DataFrame()

    for dev_eui in my_devices:
        global_df = pd.concat([global_df, df_dict[dev_eui]])

    global_df = global_df.sort_index()
    global_df = global_df[['tsr', 'DeviceID', "network.fcnt"]]
    global_df['tsr diff'] = global_df['tsr'].diff()

    # Check framecount
    fcnt_list = global_df["network.fcnt"].tolist()
    fcnt_list = list(dict.fromkeys(fcnt_list))

    for c, i in enumerate(fcnt_list):
        if c == 0:
            continue
        if i - fcnt_list[c - 1] != 1:
            print(f"Missing frame counts between {fcnt_list[c - 1]} and {i}")
            for dev in my_devices:
                failure_flags[dev] = 1

    # Check for missing data based on frequency
    devices_found = []
    first_time = 1
    for index, row in global_df.iterrows():
        if row['tsr diff'] > FREQUENCY_MS-(FREQUENCY_MS*0.2):
            if first_time:
                first_time = 0
            else:
                for dev in my_devices:
                    if dev.upper() not in devices_found:
                        failure_flags[dev] = 1
                        logger.info(
                            f"{dev}: Data missing just before {index}.")

            if row['tsr diff'] > FREQUENCY_MS+(FREQUENCY_MS*0.5):
                logger.info(f"Data missing for all nodes just before {index}.")
                for dev in my_devices:
                    failure_flags[dev] = 1

            devices_found = []
        devices_found.append(row['DeviceID'])

    global_df.to_excel(path.join(SCRIPT_PATH, 'output',
                                 'global.xlsx'),
                       index_label='Timestamp',
                       freeze_panes=(1, 0))

    for dev_eui in my_devices:
        global_df = pd.concat([global_df, df_dict[dev_eui]])
        if failure_flags[dev_eui]:
            logger.info(f"ID {dev_eui}: FAILURE!!!")
        else:
            logger.info(f"ID {dev_eui}: Pass!")
