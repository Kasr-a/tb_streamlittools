from os import path, mkdir
from lib.mCommon.thingsboard import *
import pandas as pd
import matplotlib.pyplot as plt
import yaml
import pytz
from datetime import datetime
import logging
import streamlit as st


# Expanders setup
eth = st.expander('ThingsBoard')
tm = st.expander('Start time stamp')
nl = st.expander('Node List')
hv = st.expander('Hardware Version')

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
sts = tm.date_input(label='Start Time Stamp ',
                    value=(d(year=2021, month=11, day=30)))

TimeStamp = {
    'Year': sts.year,
    'Month': sts.month,
    'Day': sts.day
}

# Debugger and mode
debug = st.checkbox('Debug',
                    help='choose whether you want to run the program in debug mode')
mode = st.selectbox('Mode', [0, 1], help='0 = BV difference on Creed Active rising edge distribution '
                                         '1 = A distribution')

# Setting up node file uploader
Nodes = nl.file_uploader('Upload Node .txt file')
blNodes = nl.file_uploader('Upload Blacklisted Nodes .txt file')
StartNode = nl.text_input('Start Node', value='260a1450')
EndNode = nl.text_input('End Node', value='260a1460')

NodeList = ''
BLNodeList = ''
if Nodes is not None:
    NodeList = Nodes.getvalue().decode('utf-8').splitlines()
if blNodes is not None:
    BLNodeList = blNodes.getvalue().decode('utf-8').splitlines()

Hw_v = hv.file_uploader('Upload Hardware version .txt file')
HardwareVersion = Hw_v.getvalue().decode('utf-8').splitlines()

if st.button('Done'):
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
        keys_list = ['data.E.payload.T.1', 'data.E.payload.T.2', 'data.E.payload.T.3',
                     'data.E.payload.T.4', 'data.E.payload.T.5', 'data.E.payload.T.6']
        start_ts = int(pytz.timezone("America/New_York").localize(
            datetime(year=int(SETTINGS["StartTimestamp"]["Year"]),
                     month=int(SETTINGS["StartTimestamp"]["Month"]),
                     day=int(SETTINGS["StartTimestamp"]["Day"]),
                     hour=int(SETTINGS["StartTimestamp"]["Hour"]))).timestamp()) * 1000
        end_ts = int(pytz.timezone("America/New_York").localize(
            datetime(year=int(SETTINGS["EndTimestamp"]["Year"]),
                     month=int(SETTINGS["EndTimestamp"]["Month"]),
                     day=int(SETTINGS["EndTimestamp"]["Day"]),
                     hour=int(SETTINGS["EndTimestamp"]["Hour"]))).timestamp()) * 1000
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
                data = get_timeseries_by_device(tb_client, this_dev, [k], start_ts=start_ts, end_ts=end_ts)

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

        # Generate graphs pt 1
        fig, ax = plt.subplots(figsize=(12, 6))
        dut_dict = df_dict['260A1859'].copy()
        dut_dict = dut_dict.dropna(subset=['data.E.payload.T.1'])
        plt.plot(dut_dict.index, dut_dict.loc[:, 'data.E.payload.T.1'], label='DUT: 260A1859 T1')

        tool_dict_T1 = df_dict['260A0365'].copy()
        tool_dict_T1 = tool_dict_T1.dropna(subset=['data.E.payload.T.1'])
        plt.plot(tool_dict_T1.index, tool_dict_T1.loc[:, 'data.E.payload.T.1'],
                 label='260A0365 Environment Temp (Left)')

        tool_dict_T2 = df_dict['260A0365'].copy()
        tool_dict_T2 = tool_dict_T2.dropna(subset=['data.E.payload.T.2'])
        plt.plot(tool_dict_T2.index, tool_dict_T2.loc[:, 'data.E.payload.T.2'],
                 label='260A0365 Environment Temp (Right)')

        tool_dict_T3 = df_dict['260A0365'].copy()
        tool_dict_T3 = tool_dict_T2.dropna(subset=['data.E.payload.T.3'])
        plt.plot(tool_dict_T3.index, tool_dict_T3.loc[:, 'data.E.payload.T.3'],
                 label='260A0365 Environment Temp (Back)')

        tool_dict_T4 = df_dict['260A0365'].copy()
        tool_dict_T4 = tool_dict_T4.dropna(subset=['data.E.payload.T.4'])
        plt.plot(tool_dict_T4.index, tool_dict_T4.loc[:, 'data.E.payload.T.4'],
                 label='260A0365 Environment Temp (Front, Below Sensor Connector)')

        tool_dict_T5 = df_dict['260A0365'].copy()
        tool_dict_T5 = tool_dict_T5.dropna(subset=['data.E.payload.T.5'])
        plt.plot(tool_dict_T5.index, tool_dict_T5.loc[:, 'data.E.payload.T.5'],
                 label='260A0365 Environment Temp (Top, Lid)')

        tool_dict_T6 = df_dict['260A0365'].copy()
        tool_dict_T6 = tool_dict_T6.dropna(subset=['data.E.payload.T.6'])
        plt.plot(tool_dict_T6.index, tool_dict_T6.loc[:, 'data.E.payload.T.6'],
                 label='260A0365 Environment Temp (Bottom, Under Bracket)')

        plt.xlabel("Timestamp")
        plt.ylabel("Temperature (°C)")
        plt.title("Temperature vs Time")
        plt.legend()

        plt.show()

        # Generate graphs pt 2
        fig, ax = plt.subplots(figsize=(12, 6))
        dut_dict = df_dict['260A1790'].copy()
        dut_dict = dut_dict.dropna(subset=['data.E.payload.T.1'])
        plt.plot(dut_dict.index, dut_dict.loc[:, 'data.E.payload.T.1'], label='DUT: 260A1790 T1')

        tool_dict_T1 = df_dict['260A0369'].copy()
        tool_dict_T1 = tool_dict_T1.dropna(subset=['data.E.payload.T.1'])
        plt.plot(tool_dict_T1.index, tool_dict_T1.loc[:, 'data.E.payload.T.1'],
                 label='260A0369 Environment Temp (Left)')

        tool_dict_T2 = df_dict['260A0369'].copy()
        tool_dict_T2 = tool_dict_T2.dropna(subset=['data.E.payload.T.2'])
        plt.plot(tool_dict_T2.index, tool_dict_T2.loc[:, 'data.E.payload.T.2'],
                 label='260A0369 Environment Temp (Right)')

        tool_dict_T3 = df_dict['260A0369'].copy()
        tool_dict_T3 = tool_dict_T2.dropna(subset=['data.E.payload.T.3'])
        plt.plot(tool_dict_T3.index, tool_dict_T3.loc[:, 'data.E.payload.T.3'],
                 label='260A0369 Environment Temp (Back)')

        tool_dict_T4 = df_dict['260A0369'].copy()
        tool_dict_T4 = tool_dict_T4.dropna(subset=['data.E.payload.T.4'])
        plt.plot(tool_dict_T4.index, tool_dict_T4.loc[:, 'data.E.payload.T.4'],
                 label='260A0369 Environment Temp (Front, Below Sensor Connector)')

        tool_dict_T5 = df_dict['260A0369'].copy()
        tool_dict_T5 = tool_dict_T5.dropna(subset=['data.E.payload.T.5'])
        plt.plot(tool_dict_T5.index, tool_dict_T5.loc[:, 'data.E.payload.T.5'],
                 label='260A0369 Environment Temp (Top, Lid)')

        tool_dict_T6 = df_dict['260A0369'].copy()
        tool_dict_T6 = tool_dict_T6.dropna(subset=['data.E.payload.T.6'])
        plt.plot(tool_dict_T6.index, tool_dict_T6.loc[:, 'data.E.payload.T.6'],
                 label='260A0369 Environment Temp (Bottom, Under Bracket)')

        plt.xlabel("Timestamp")
        plt.ylabel("Temperature (°C)")
        plt.title("Temperature vs Time")
        plt.legend()

        plt.show()
