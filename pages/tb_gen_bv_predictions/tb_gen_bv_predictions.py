from os import path, mkdir
from lib.mCommon.thingsboard import *
import pandas as pd
import matplotlib.pyplot as plt
import yaml
import pytz
from datetime import datetime, timedelta
import logging
import numpy as np
import matplotlib.dates as mdates


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

    # Variable generation for while loop
    keys_list = ['data.N.payload.BV']
    if SETTINGS["HardwareVersions"]:
        keys_list.append('data.D.payload.ASCII.1.brd_ver')
    start_ts = int(pytz.timezone("America/New_York").localize(
        datetime(year=SETTINGS["StartTimestamp"]["Year"],
                 month=SETTINGS["StartTimestamp"]["Month"],
                 day=SETTINGS["StartTimestamp"]["Day"],
                 hour=SETTINGS["StartTimestamp"]["Hour"])).timestamp()) * 1000
    end_ts = int(pytz.timezone("America/New_York").localize(
        datetime(year=SETTINGS["EndTimestamp"]["Year"],
                 month=SETTINGS["EndTimestamp"]["Month"],
                 day=SETTINGS["EndTimestamp"]["Day"],
                 hour=SETTINGS["EndTimestamp"]["Hour"])).timestamp()) * 1000
    fig, ax = plt.subplots(figsize=(12, 6))

    # While loop to pull data and fill battery differences distributions dict
    for dev_eui in my_devices:
        logger.info('processing %s ==========================================================' % dev_eui)
        try:
            this_dev_spec = my_devices_specs[dev_eui]
        except KeyError:
            logger.warning("Node id %s not found in thingsboard. Moving on." % dev_eui)
            continue
        this_dev = DeviceId(id=this_dev_spec['id']['id'], entity_type=this_dev_spec['id']['entityType'])

        # Get data into dataframe for each key
        df_final = pd.DataFrame()
        for k in keys_list:
            logger.debug('processing key %s' % k)
            data = get_timeseries_by_device(tb_client, this_dev, [k], start_ts=start_ts, end_ts=end_ts)

            df_data = pd.DataFrame.from_records(data)
            if df_data.empty:
                logger.debug("Empty dataframe.")
                continue
            df_data['ts'] = pd.to_datetime(df_data['ts'], unit='ms')
            if k != 'data.D.payload.ASCII.1.brd_ver':
                df_data['value'] = df_data['value'].apply(pd.to_numeric)
            df_data.set_index('ts', inplace=True)
            df_data.rename(columns={"value": k}, inplace=True)

            df_final = pd.concat([df_final, df_data], axis=1)
            logger.debug('%s: %d' % (k, len(data)))

        # Remove device ids that don't match specified hardware versions
        if SETTINGS["HardwareVersions"]:
            if 'data.D.payload.ASCII.1.brd_ver' not in df_final.columns:
                logger.debug("No hardware version data found.")
                continue
            df_no_none_brd_ver = df_final.copy()
            df_no_none_brd_ver = df_no_none_brd_ver.dropna(subset=['data.D.payload.ASCII.1.brd_ver'])
            if df_no_none_brd_ver.iloc[-1]['data.D.payload.ASCII.1.brd_ver'] in SETTINGS["HardwareVersions"]:
                pass
            else:
                logger.debug("Device not the correct Skyla Hardware Version: %s" % df_no_none_brd_ver.iloc[-1][
                    'data.D.payload.ASCII.1.brd_ver'])
                continue

        df_final.to_excel(path.join(SCRIPT_PATH, 'output',
                                   '%s_%s-%s.xlsx' % (dev_eui, start_ts, end_ts)),
                         index_label='Timestamp',
                         freeze_panes=(1, 0))

        # Generate predictions for battery voltage
        df_final["raw_ts"] = df_final.index
        x_values = []
        for index, row in df_final.iterrows():
            utc_time = datetime.strptime(str(row['raw_ts']), "%Y-%m-%d %H:%M:%S")
            x_values.append(int((utc_time - datetime(1970, 1, 1)).total_seconds()))

        y_values = df_final["data.N.payload.BV"]
        coeffs = np.polyfit(x_values, y_values, 1)
        poly_eqn = np.poly1d(coeffs)

        logger.info(dev_eui+" equation: "+str(poly_eqn))

        y_hat = poly_eqn(x_values)

        plt.plot(df_final.index, df_final.loc[:, "data.N.payload.BV"], label=dev_eui+"_bv")
        # plt.plot(df_final.index, df_final.loc[:, "data.N.payload.BV"])

        # plt.plot(df_final.index, y_hat, label=dev_eui+"_lobf")

        backwards_coeffs = np.polyfit(y_values, x_values, 1)
        backwards_poly_eqn = np.poly1d(backwards_coeffs)
        predicted_ts = int(backwards_poly_eqn(3.1))
        pred_ts_str = datetime.fromtimestamp(predicted_ts)

        logger.info(dev_eui + " start date: " + str(df_final.iloc[-1]['raw_ts']))
        logger.info(dev_eui+" drop date: "+str(pred_ts_str))
        duration = (pred_ts_str - df_final.iloc[-1]['raw_ts']).days
        logger.info(dev_eui + " Duration: " + str(duration))

        if SETTINGS["TripledNodeList"] and dev_eui.lower() in SETTINGS["TripledNodeList"]:
            duration = (pred_ts_str - df_final.iloc[-1]['raw_ts']).days*3
            logger.info(dev_eui + " Tripled Duration: " + str(duration))
            new_drop_date = df_final.iloc[-1]['raw_ts'] + timedelta(days=duration)
            logger.info(dev_eui + " Tripled drop date: " + str(new_drop_date))

            pred_x3 = [df_final.index[-1], new_drop_date]
            pred_y3 = [df_final["data.N.payload.BV"][-1], 3.1]
            # plt.plot(pred_x3, pred_y3, label=dev_eui + "_prediction_x3")
            ax.annotate(str(new_drop_date)[:10], (mdates.date2num(new_drop_date), 3.1), ha="center", va="top", fontsize=7)
        else:
            pred_x = [df_final.index[-1], pred_ts_str]
            pred_y = [df_final["data.N.payload.BV"][-1], 3.1]
            # plt.plot(pred_x, pred_y, label=dev_eui+"_prediction")
            ax.annotate(str(pred_ts_str)[:10], (mdates.date2num(pred_ts_str), 3.1), ha="center", va="top", fontsize=7)

    plt.xlabel("Timestamp")
    plt.ylabel("Voltage (V)")
    plt.title("Battery Voltage Predictions")

    plt.legend()

    # plt.show()

    plt.savefig(path.join(path.join(SCRIPT_PATH, SETTINGS["OutputFolder"]),
                          f"BV_{SETTINGS['StartNode']}-{SETTINGS['EndNode']}_{start_ts}-{end_ts}"))
