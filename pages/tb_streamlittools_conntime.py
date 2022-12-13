from os import path, mkdir
from lib.mCommon.thingsboard import *
import pandas as pd
import yaml
import pytz
from datetime import datetime
import logging
import streamlit as st
from datetime import date as d
from datetime import time as t
from datetime import datetime as dt

# Setting up expanders
eth = st.expander('ThingsBoard')
nl = st.expander('Node List')
et = st.expander('Misc')
tm = st.expander('Start/End time')


# Thingsboard setup
tb_keylist = ['Host', 'Port', 'Username', 'Password', 'CustomerName']
ThingsBoard = dict(zip(tb_keylist, [None] * len(tb_keylist)))
eth.subheader('ThingsBoard')
ThingsBoard['Host'] = eth.text_input('Please Enter Host',
                                     placeholder='http://ops.aoms-tech.com', value='http://ops.aoms-tech.com')

ThingsBoard['Port'] = eth.number_input('Please Enter Port', step=10,
                                       value=8080)

ThingsBoard['Username'] = eth.text_input('Please Enter Username',
                                         placeholder='hardware@aoms-tech.com', value='hardware@aoms-tech.com')

ThingsBoard['Password'] = eth.text_input('Please Enter Password', type='password',
                                         placeholder='Zhr9byhcwMt_YZLPY!_ta@', value='Zhr9byhcwMt_YZLPY!_ta@')

ThingsBoard['CustomerName'] = eth.text_input('Please Enter Customer Name',
                                             placeholder='AOMS OPS', value='AOMS OPS')

# Date picker setup
sed = tm.date_input(label='Start-End date ',
                    value=(d(year=2022, month=10, day=28),
                           d(year=2022, month=10, day=31)),
                    key='date_range')


hms = tm.time_input('Start time',
                    value=(t(hour=19, minute=0)),
                    key='start_time')
hme = tm.time_input('End time',
                    value=(t(hour=8, minute=0)),
                    key='end_time')


finSD = dt.combine(sed[0], hms)
finED = dt.combine(sed[1], hme)
sDate = {
    'Year': finSD.year,
    'Month': finSD.month,
    'Day': finSD.day,
    'Hour': finSD.hour,
    'Minute': finSD.minute
}
eDate = {
    'Year': finED.year,
    'Month': finED.month,
    'Day': finED.day,
    'Hour': finED.hour,
    'Minute': finED.day
}

# Debugger
debug = st.checkbox('Debug',
                    help='choose whether you want to run the program in debug mode')

# Misc expander content
HighTempThreshold = et.number_input('High Temperature Threshold', value=28)
LowTempThreshold = et.number_input('Low Temperature Threshold', value=21)
FrequencyMS = et.number_input('FrequencyMS', value=900000)
OutputFolder = et.text_input('Output Folder Name', placeholder='output', value='output')

# Setting up node file uploader
up = nl.file_uploader('Upload Node .txt file')
if up is not None:
    NodeList = up.getvalue().decode('utf-8').splitlines()

    if st.button('Done'):
        st.write("Device ID          Num of Occurences          Lengths of occurence")
        SCRIPT_PATH = path.dirname(path.realpath(__file__))
        # SETTINGS_PATH = path.join(SCRIPT_PATH, 'settings.yaml')
        # with open(SETTINGS_PATH, 'r', encoding='utf-8') as setting:
        #     SETTINGS = yaml.safe_load(setting)

        if debug:
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
                mkdir(path.join(SCRIPT_PATH, OutputFolder))
            except:
                pass

            # Login to thingsboard
            TB_URL = ThingsBoard['Host'] + ":" + str(ThingsBoard['Port'])
            tb_client = RestClientPE(base_url=TB_URL)
            tb_client.login(username=ThingsBoard['Username'], password=ThingsBoard['Password'])

            # Generate device spec dictionary
            if type(NodeList) == list:
                my_devices = NodeList
            else:
                my_devices = [NodeList]

            device_list = get_devices_by_customer_name_as_dict(tb_client, ThingsBoard['CustomerName'])
            my_devices_specs = {k: device_list[k] for k in device_list if k in my_devices}
            logger.debug(my_devices_specs)

            # Generate while loop variables
            keys_list = ['data.N.payload.ASCII.1.conn_time']
            start_ts = int(pytz.timezone("America/New_York").localize(
                datetime(year=int(sDate['Year']),
                         month=int(sDate['Month']),
                         day=int(sDate['Day']),
                         hour=int(sDate['Hour']),
                         minute=int(sDate['Minute']))).timestamp()) * 1000
            end_ts = int(pytz.timezone("America/New_York").localize(
                datetime(year=int(eDate['Year']),
                         month=int(eDate['Month']),
                         day=int(eDate['Day']),
                         hour=int(eDate['Hour']),
                         minute=int(eDate['Minute']))).timestamp()) * 1000

            HIGH_TEMP_THRESHOLD = HighTempThreshold
            LOW_TEMP_THRESHOLD = LowTempThreshold
            FREQUENCY_MS = FrequencyMS

            df_dict = {}
            failure_flags = {}

            # While loop to pull data and fill dicts
            for dev_eui in my_devices:
                failure_flags[dev_eui] = 0
                logger.debug('Processing %s ...' % dev_eui)
                try:
                    this_dev_spec = my_devices_specs[dev_eui]
                except KeyError:
                    logger.warning("Node id %s not found in ThingsBoard. Moving on." % dev_eui)
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

                    df_data.set_index('ts', inplace=True)

                    df_dict[dev_eui] = pd.concat([df_dict[dev_eui], df_data], axis=1)
                    logger.debug('%s: %d' % (k, len(data)))

                # Finalize df
                df_dict[dev_eui]["DeviceID"] = dev_eui
                df_dict[dev_eui] = df_dict[dev_eui].sort_index(ascending=True)
                df_dict[dev_eui]['tsr diff'] = df_dict[dev_eui]['tsr'].diff()

                # Check for missing data based on frequency
                devices_found = []
                first_time = 1
                number_messages_missed = 0
                max_missed_interval = 0
                for index, row in df_dict[dev_eui].iterrows():
                    if row['tsr diff'] > FREQUENCY_MS + (FREQUENCY_MS * 0.5):
                        if row['tsr diff'] > max_missed_interval:
                            max_missed_interval = row['tsr diff']
                        logger.info(f"{dev_eui}: Data missing just before {index}.")
                        failure_flags[dev_eui] = 1
                        number_messages_missed += 1

                        devices_found = []
                    devices_found.append(row['DeviceID'])

                if failure_flags[dev_eui]:
                    st.text(
                        f"{dev_eui}          {number_messages_missed}          {max_missed_interval / (1000 * 60 * 60)}")

                df_dict[dev_eui].to_excel(path.join(SCRIPT_PATH, 'output',
                                                    '%s_%s-%s.xlsx' % (dev_eui, start_ts, end_ts)),
                                          index_label='Timestamp',
                                          freeze_panes=(1, 0))

            for dev_eui in my_devices:
                if failure_flags[dev_eui]:
                    logger.info(f"ID {dev_eui}: FAILURE!!!")
                else:
                    logger.info(f"ID {dev_eui}: Pass!")

else:
    st.warning('Upload Node File')
