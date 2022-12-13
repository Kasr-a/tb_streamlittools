## Common helper functions to interface with thingsboard
from datetime import datetime
import time
import logging
from tb_rest_client32.rest_client_pe import RestClientPE
from tb_rest_client32.models.models_ce import DeviceId

logger = logging.getLogger(__name__)


def get_assets_by_customer_name_as_dict(tb_client: RestClientPE, customer_name: str) -> dict:
    target_customer = tb_client.get_tenant_customer(customer_name)

    customer_assets = []
    i_page = 0

    while True:
        resp = tb_client.get_customer_assets(target_customer.id, page=i_page)
        customer_assets.extend(resp.data)

        i_page += 1
        if i_page > resp.total_pages:
            break

    # convert to dict
    d = {obj.name: obj for obj in customer_assets}

    return d


def get_devices_by_customer_name_as_dict(tb_client: RestClientPE, customer_name: str) -> dict:
    target_customer = tb_client.get_tenant_customer(customer_name)

    customer_devices = []
    i_page = 0

    while True:
        resp = tb_client.get_customer_devices(target_customer.id, page=i_page)
        customer_devices.extend(resp.data)

        i_page += 1
        if i_page > resp.total_pages:
            break

    dict_all_devices = {d['name']: d for d in customer_devices}

    return dict_all_devices


# todo:
def get_devices_by_asset_name_as_dict():
    pass


# Helper function to get all timeseries data
def get_timeseries_all(tb_client: RestClientPE, dev_id:DeviceId, key:str,
                       start_ts: int, end_ts: int = int(time.time())*1000, page_limit: int = 1000,
                       use_strict_data_types:bool = False,
                       timeout_seconds: int = 20):

    my_end_ts = end_ts
    i = 1
    raw = []

    exec_start_ts = time.time()

    while True:
        logger.debug('get_timeseries_all(): pass %d, end_ts = %d | %s' % (i, my_end_ts,
                                               datetime.fromtimestamp(float(my_end_ts/1000)).strftime('%Y-%m-%d %H:%M:%S')))
        i += 1

        data = tb_client.get_timeseries(dev_id, key, use_strict_data_types=use_strict_data_types,
                                        start_ts=start_ts, end_ts=my_end_ts,
                                        limit=page_limit)

        if len(data) == 0:
            logger.debug('get_timeseries_all(): no more data to download')
            break

        if exec_start_ts - time.time() > timeout_seconds:
            logger.warning('get_timeseries_all(): exceeded timeout %d' % timeout_seconds)
            break

        raw.extend(data[key])

        min_ts = min([d['ts'] for d in data[key]])
        if min_ts >= start_ts:
            my_end_ts = min_ts - 1    #subtract 1 ms to minimum time received as new end ts

    logger.debug("get_timeseries_all(): a total of %d values received" % len(raw))

    return raw


def get_timeseries_by_device(tb_client: RestClientPE, this_dev: DeviceId, lst_key: list, start_ts: int = 1, end_ts: int = int(time.time()*1000)):

    lst_raw = []
    for k in lst_key:
        logger.debug('get_timeseries_by_device(): processing key %s' % k)
        resp = get_timeseries_all(tb_client, this_dev, k, start_ts=start_ts, end_ts=end_ts, page_limit=5000)
        # resp has this format: # {"data.E.raw": {'ts': 1644846652219,'value': 'abc'}, {'ts': 1644839452413,'value': 'efg'}}
        lst_raw.extend(resp)
        logger.debug("get_timeseries_by_device(): total values %d" % len(lst_raw))

    return lst_raw


if __name__ == "__main__":

    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.DEBUG,
                        handlers=[logging.StreamHandler()
                        ])
    logger = logging.getLogger()

    TB_HOST = "http://ops.aoms-tech.com" # Community Edition, ops dashboard
    TB_PORT = "8080"
    TB_URL = TB_HOST + ":" + TB_PORT

    # Default Tenant Administrator credentials
    TB_USERNAME = "hardware@aoms-tech.com"
    TB_PASSWORD = "Zhr9byhcwMt_YZLPY!_ta@"

    CUSTOMER_NAME = "AOMS OPS"

    tb_client = RestClientPE(base_url=TB_URL)
    tb_client.login(username=TB_USERNAME, password=TB_PASSWORD)

    lst_dev = get_devices_by_customer_name_as_dict(tb_client, CUSTOMER_NAME)

    my_devices = ["260A0180", "260A0181", "260A0211"]

    my_devices_spec = {k:lst_dev[k] for k in lst_dev if k in my_devices}

    logger.info(my_devices_spec)
    lst_keys = ['data.E.raw', 'data.N.raw', 'data.I.raw', 'data.A.raw', 'data.D.raw']

    import pytz
    start_ts = int(pytz.timezone("America/New_York").localize(datetime(year=2022, month=3, day=8, hour=9)).timestamp())*1000

    dict_raw = {}
    for dev_eui in my_devices:
        logger.debug('processing %s' % dev_eui)
        this_dev_spec = my_devices_spec[dev_eui]
        this_dev = DeviceId(id=this_dev_spec['id']['id'], entity_type=this_dev_spec['id']['entityType'])

        data = get_timeseries_by_device(tb_client, this_dev, lst_keys, start_ts=start_ts)

        dict_raw[dev_eui] = data
        logger.debug('%s: %d' % (dev_eui, len(data)))
        break

    # test code
    pass