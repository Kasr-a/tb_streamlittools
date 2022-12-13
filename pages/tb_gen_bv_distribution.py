from os import path, mkdir
from lib.mCommon.thingsboard import *
import pandas as pd
import matplotlib as plt
import yaml
import pytz
from datetime import datetime
import logging
import streamlit as st
from datetime import datetime as dt
from datetime import date as d
from datetime import time as t

st.text(f'matplotlib: {plt.__version__}')
# MODE = 0 ==> BV difference on Creed Active rising edge distribution
# MODE = 1 ==> A distribution

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
if Hw_v is not None:
    HardwareVersion = Hw_v
if st.button('Done'):
    SCRIPT_PATH = path.dirname(path.realpath(__file__))
    SETTINGS_PATH = path.join(SCRIPT_PATH, 'settings.yaml')
    with open(SETTINGS_PATH, 'r', encoding='utf-8') as settings:
        SETTINGS = yaml.safe_load(settings)

    if debug: 
        logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO,
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
                if long_id not in BLNodeList:
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
        TB_URL = ThingsBoard['Host'] + ":" + str(ThingsBoard['Port'])
        tb_client = RestClientPE(base_url=TB_URL)
        tb_client.login(username=ThingsBoard['Username'], password=ThingsBoard['Password'])

        # Generate device spec dictionary
        if NodeList:
            if type(NodeList) == list:
                my_devices = NodeList
            else:
                my_devices = [NodeList]
        else:
            my_devices = get_node_list_from_startid_and_endid(StartNode, EndNode)

        device_list = get_devices_by_customer_name_as_dict(tb_client, ThingsBoard['CustomerName'])
        my_devices_specs = {k: device_list[k] for k in device_list if k in my_devices}
        logger.info(my_devices_specs)

        # Variable generation for while loop
        if mode:
            keys_list = ['data.E.payload.A']
            distribution_vals = {"Neg": []}
            for n in range(500, 2010, 10):
                distribution_vals[str(n)] = []
            distribution_vals["2000+"] = []
            aggregate_title = "Minimum analog reading"
        else:
            keys_list = ['data.N.payload.BV', 'data.N.payload.ASCII.1.creed_active']
            distribution_vals = {"Neg": []}
            for n in range(0, 30):
                distribution_vals[str(n / 10)] = []
            aggregate_title = "Maximum battery difference"

        if HardwareVersion:
            keys_list.append('data.D.payload.ASCII.1.brd_ver')

        start_ts = int(pytz.timezone("America/New_York").localize(
            datetime(year=TimeStamp['Year'], month=TimeStamp['Month'],
                     day=TimeStamp['Day'], hour=0)).timestamp()) * 1000

        first_found_aggregate_val = 1
        aggregate_val = 0
        # While loop to pull data and fill battery differences distributions dict
        for dev_eui in my_devices:
            logger.info('processing %s' % dev_eui)
            try:
                this_dev_spec = my_devices_specs[dev_eui]
            except KeyError:
                logger.warning("Node id %s not found in thingsboard. Moving on." % dev_eui)
                continue
            this_dev = DeviceId(id=this_dev_spec['id']['id'], entity_type=this_dev_spec['id']['entityType'])

            # Get data into dataframe for each key
            df_final = pd.DataFrame()
            for k in keys_list:
                logger.info('processing key %s' % k)
                data = get_timeseries_by_device(tb_client, this_dev, [k], start_ts=start_ts)

                df_data = pd.DataFrame.from_records(data)
                if df_data.empty:
                    logger.info("Empty dataframe.")
                    continue
                df_data['ts'] = pd.to_datetime(df_data['ts'], unit='ms')
                if k != 'data.D.payload.ASCII.1.brd_ver':
                    df_data['value'] = df_data['value'].apply(pd.to_numeric)
                df_data.set_index('ts', inplace=True)
                df_data.rename(columns={"value": k}, inplace=True)

                df_final = pd.concat([df_final, df_data], axis=1)
                logger.info('%s: %d' % (k, len(data)))

            # Check for necessary data in dataframe
            if not mode:
                if "data.N.payload.ASCII.1.creed_active" not in df_final.columns:
                    logger.info("No creed active data found.")
                    continue

            if HardwareVersion:
                if 'data.D.payload.ASCII.1.brd_ver' not in df_final.columns:
                    logger.info("No hardware version data found.")
                    continue
                df_no_none_brd_ver = df_final.copy()
                df_no_none_brd_ver = df_no_none_brd_ver.dropna(subset=['data.D.payload.ASCII.1.brd_ver'])
                if df_no_none_brd_ver.iloc[-1]['data.D.payload.ASCII.1.brd_ver'] in HardwareVersion:
                    pass
                else:
                    logger.info("Device not the correct Skyla Hardware Version: %s" % df_no_none_brd_ver.iloc[-1][
                        'data.D.payload.ASCII.1.brd_ver'])
                    continue

            # Generate new columns in dataframe
            if not mode:
                df_final['Toggle'] = df_final["data.N.payload.ASCII.1.creed_active"].diff(periods=-1)
                df_final['Batt Diff'] = df_final["data.N.payload.BV"].diff(periods=-1) / -1

                # df_final.to_excel(path.join(SCRIPT_PATH, 'output',
                #                            '%s_%s.xlsx' % (dev_eui, start_ts)),
                #                  index_label='Timestamp',
                #                  freeze_panes=(1, 0))

                df_final = df_final.loc[df_final['Toggle'] == 1]

                # Fill in battery differences distribution dict and find max battery difference
                for val in df_final["Batt Diff"].tolist():
                    if val > 0.0:
                        distribution_vals["{:.1f}".format(val)].append(val)
                    else:
                        distribution_vals["Neg"].append(val)
                max_val = df_final["Batt Diff"].max()
                if max_val > aggregate_val:
                    aggregate_val = max_val
            else:
                for val in df_final["data.E.payload.A"]:
                    if val >= 2000:
                        distribution_vals["2000+"].append(val)
                    elif val >= 0:
                        distribution_vals[f"{round(val, -1)}"].append(val)
                    else:
                        distribution_vals["Neg"].append(val)
                min_val = df_final["data.E.payload.A"].min()
                if first_found_aggregate_val:
                    aggregate_val = min_val
                    first_found_aggregate_val = 0
                elif min_val < aggregate_val:
                    aggregate_val = min_val

        logger.info("%s: %f" % (aggregate_title, aggregate_val))

        # PLot battery differences distribution as bar graph
        x = []
        y = []
        init = 1
        for key in distribution_vals.keys():
            if init:
                init = 0
                if distribution_vals[key]:
                    y = [len(distribution_vals[key])]
                    x = [key]
            else:
                if distribution_vals[key]:
                    y.append(len(distribution_vals[key]))
                    x.append(key)

        if mode:
            x = x[:20]
            y = y[:20]
        else:
            x = x[-20:]
            y = y[-20:]

        fig, ax = plt.pyplot.subplots(figsize=(12, 6))
        bars = ax.bar(x, y)
        ax.bar_label(bars)

        plt.pyplot.title("%s" % aggregate_title)
        plt.pyplot.xlabel("Voltage Groups")
        plt.pyplot.ylabel("Number of Occurrences")

        text_x = 0
        text_y = 0

        if HardwareVersion:
            plt.pyplot.figtext(0.5, 0.01,
                               "%s: %f ; Hardware Versions Included: %s" % (aggregate_title,
                                                                            aggregate_val, HardwareVersion),
                               ha="center", va="center", fontsize=18,
                               bbox={"facecolor": "blue", "alpha": 0.5})
        else:
            plt.pyplot.figtext(0.5, 0.01, "%s: %f" % (aggregate_title, aggregate_val), ha="center", va="center",
                               fontsize=18,
                               bbox={"facecolor": "blue", "alpha": 0.5})

        if mode:
            plt.pyplot.savefig(
                path.join(path.join(SCRIPT_PATH, SETTINGS["OutputFolder"]), f"A_{my_devices[0]}_{start_ts}"))
        else:
            plt.pyplot.savefig(path.join(path.join(SCRIPT_PATH, SETTINGS["OutputFolder"]),
                                         f"BV_{StartNode}-{EndNode}_{start_ts}"))
