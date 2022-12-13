import jwt
from datetime import datetime, timedelta
import time
import requests
import logging
import string
from lib.mCommon.tools import ping
logger = logging.getLogger(__name__)


def is_hex(s):
    return all(c in string.hexdigits for c in s)

# ====================== APP SERVER ====================================

class Flora(object):
    '''
    This class uses the Requests library to directly call REST endpoint APIs based on swagger documentations on /api endpoint

    '''
    def __init__(self, host, port, secret_token):
        '''
        :param host: host name of chiprstack server
        :param port: port number of chirpstack server
        :param secret_token: jwt secret token as specified in the chirpstack_app_server.conf file on server instance
        '''
        logger.debug('Creating a flora object Host=%s:%s' % (host, port))
        self._host = host
        self._port = port
        self._secret_token = secret_token
        self._jwt_token = self._get_token()

    def update_token(self):
        self._jwt_token = self._get_token()

    # Get security token to access fLora Server
    def _get_token(self):
        # Get token start time, make it a bit earlier than now
        time_now = datetime.utcnow() - timedelta(seconds=10)         # this is required

        # https://jwt.io/
        payload = {
          "iss": "lora-app-server",
          "aud": "lora-app-server",
          "nbf": time_now,
        #   "exp": time_end,
          "sub": "user",
          "username": "admin"
        }
        jwt_token = jwt.encode(payload, self._secret_token, algorithm='HS256')

        return jwt_token

    def ping(self):
        return ping(self._host)

    def _url(self, ep):
        return 'http://' + self._host + ':' + self._port + '/api' + ep

    def _header(self):
        return {'Grpc-Metadata-Authorization': self._jwt_token}

    def get_applications_count(self):
        return int(requests.get(self._url('/applications'), headers=self._header()).json()['totalCount'])

    def get_applications_list_as_dictionary(self):
        cnt = self.get_applications_count()
        set = {}

        if cnt < 1:
            logger.warning("No application found")

        for i in range(cnt):
            p = {"limit": "1", "offset": str(i)}

            val = requests.get(self._url('/applications'), headers=self._header(), params=p).json()["result"][0]
            key = val["name"]
            set[key] = val["id"]

        return set

    def get_applications_by_id(self, id_):
        return requests.get(self._url('/applications/{:d}'.format(id_)), headers=self._header()).json()

    def get_organizations_count(self):
        return int(requests.get(self._url('/organizations'), headers=self._header()).json()['totalCount'])

    def get_organizations_list_as_dictionary(self):
        cnt = self.get_organizations_count()
        set = {}

        if cnt < 1:
            logger.warning("No organizations found")

        for i in range(cnt):
            p = {"limit": "1", "offset": str(i)}

            val = requests.get(self._url('/organizations'), headers=self._header(), params=p).json()["result"][0]
            key = val["name"]
            set[key] = val["id"]

        return set

    def get_organizations_by_id(self, id_):
        return requests.get(self._url('/organizations/{:d}'.format(id_)), headers=self._header()).json()

    def get_gateways_by_organization_id(self, org_id):

        try:
            p = {"organizationID": str(org_id)}
            r = requests.get(self._url('/gateways'), headers=self._header(), params=p)
            time.sleep(0.1)
            r.raise_for_status()
            count = int(r.json()['totalCount'])
        except:
            logger.warning('unable to get gateway count [code]: ' + str(r.status_code))
            return {}

        dict_gateway = {}

        for i in range(0, count):

            try:
                p = {"organizationID": str(org_id),
                     "limit": '1',
                     "offset": str(i)}
                r = requests.get(self._url('/gateways'), headers=self._header(), params=p)
                time.sleep(0.1)
                r.raise_for_status()

                eui = str(r.json()['result'][0]['name'])
                gid = str(r.json()['result'][0]['id'])

                dict_gateway[eui] = gid

            except:
                logger.error('unable to to get gateway detail, index=%d [code]: %s' % (i, str(r.status_code)))

        return dict_gateway

    def get_devices_count(self):
        try:
            r = requests.get(self._url('/devices'), headers=self._header())
            time.sleep(0.1)
            r.raise_for_status()
            return int(r.json()['totalCount'])
        except:
            logger.error('get_devices_count [code]: ' + str(r.status_code))
            return 0

    def get_devices_count_by_application(self, app_id):
        try:
            p = {"applicationID": str(app_id)}
            r = requests.get(self._url('/devices'), headers=self._header(), params=p)
            #time.sleep(0.1)
            r.raise_for_status()
            return int(r.json()['totalCount'])
        except:
            logger.error('get_devices_count [code]: ' + str(r.status_code))
            return 0

    # Returns a dictionary of devices
    # Key = device EUI
    # Value = device address
    def get_devices_by_application(self, app_id):

        cnt = int(self.get_devices_count_by_application(app_id))
        set = {}

        if cnt < 1:
            logger.warning("No device found in application %s" % str(app_id))

        for i in range(cnt):
            p = {"limit": "1", "offset": str(i), "applicationID": str(app_id)}

            val = requests.get(self._url('/devices'), headers=self._header(), params=p).json()["result"][0]
            key = val["devEUI"]
            set[key] = val["name"]

        return set

    def get_activation_by_devEUI(self, devEUI):
        try:
            if len(devEUI) != 16:
                return 0

            r = requests.get(self._url('/devices/%s/activation' % devEUI), headers=self._header())
            # time.sleep(0.1)
            r.raise_for_status()
            return r.json()
        except:
            logger.error('get_devices_activation %s [code]: ' % devEUI + str(r.status_code))
            return 0

    def get_activation_by_application(self, app_id):
        logger.info('Getting devices for Application #%s', app_id)
        devices = self.get_devices_by_application(app_id)

        logger.info('Get activation')
        logger.info('DEV EUI          | APPLICATION_KEY                  NETWORK_KEY')

        dev_act = {}

        for devEUI, devAdrs in sorted(devices.items()):

            r = self.get_activation_by_devEUI(devEUI)

            dev_act[devEUI] = {}

            if not r:
                logger.debug('no activation found for %s' % devEUI)
                app_key = ''
                nwk_key = ''
                fcnt_down = 0
                fcnt_up = 0
            else:
                app_key = r['deviceActivation']['appSKey'].upper()
                nwk_key = r['deviceActivation']['nwkSEncKey'].upper()
                fcnt_down = int(r['deviceActivation']['nFCntDown'])
                fcnt_up = int(r['deviceActivation']['fCntUp'])

            dev_act[devEUI]['dev_address'] = devAdrs
            dev_act[devEUI]['app_key'] = app_key
            dev_act[devEUI]['nwk_key'] = nwk_key
            dev_act[devEUI]['fcnt_down'] = fcnt_down
            dev_act[devEUI]['fcnt_up'] = fcnt_up

            # logger.info('%s | %s %s' % (devEUI,
            #                             r['deviceActivation']['appSKey'],
            #                             r['deviceActivation']['nwkSEncKey']))

        return dev_act

    def get_device_profiles_count(self):
        try:
            r = requests.get(self._url('/device-profiles'), headers=self._header())
            time.sleep(0.1)
            r.raise_for_status()
            return int(r.json()['totalCount'])
        except:
            logger.error('get_devices_count [code]: ' + str(r.status_code))
            return 0

    def get_device_profiles_list_as_dictionary(self):

        cnt = int(self.get_device_profiles_count())
        set = {}

        if cnt < 1:
            logger.warning("No device profiles found")

        for i in range(cnt):
            p = {"limit": "1", "offset": str(i)}

            val = requests.get(self._url('/device-profiles'), headers=self._header(), params=p).json()["result"][0]
            key = val["name"]
            set[key] = val["id"]

        return set

    def get_missing_activation_by_application(self, app_id):
        logger.debug('Search and log devices with missing activation for application #%s', app_id)
        devices = self.get_devices_by_application(app_id)

        dict = {}
        for devEUI, devAdrs in sorted(devices.items()):

            r = self.get_activation_by_devEUI(devEUI)

            if not r:
                dict[devEUI] = devAdrs

        return dict

    # devEUI should be supplied as a string
    def delete_device_by_devEUI(self, devEUI):
        try:
            r = requests.delete(self._url('/devices/{:s}'.format(devEUI)), headers=self._header())
            r.raise_for_status()
            return True
        except:
            logger.error('delete_device_by_devEUI [code]: ' + str(r.status_code))
            return False

    def delete_devices_by_application(self, app_id):
        s = self.get_devices_by_application(app_id)
        r = 1

        for devEUI, devAddress in s.items():
            logger.info("deleting device: " + devEUI + " - " + devAddress)
            r &= self.delete_device_by_devEUI(devEUI)

        return r

    def add_device(self, body):
        try:
            r = requests.post(self._url('/devices'), headers=self._header(), json=body)
            r.raise_for_status()
            return True

        except:
            logger.error('add_device [code]: ' + str(r.status_code))
            logger.error("device_eui=" + body["device"]["devEUI"] + ": " + r.json()["message"])
            return False

    # Given a dictionary (key=dev_EUI, value=dev_address) and application ID,
    # add all the devices to that particular application ID.
    def add_devices_by_application(self, dict_, app_id, dev_profile_id):

        r = 1

        for devEUI, devAddress in sorted(dict_.items()):
            logger.info("adding device: " + devEUI + " - " + devAddress)
            device_spec = {
                "device": {
                    "applicationID": app_id,
                    "description": devAddress,
                    "devEUI": devEUI,
                    "deviceProfileID": dev_profile_id,
                    "name": devAddress,
                    "referenceAltitude": 0,
                    "skipFCntCheck": True
                }
            }

            r &= self.add_device(device_spec)

            if not r:
                logger.warning("add device by application failed")

        return r

    def add_device_keys(self, dev_eui, app_key, gen_app_key, nwk_key):
        '''
        This function is used for adding keys for OTAA devices
        :param dev_eui:
        :param app_key:
        :param gen_app_key:
        :param nwk_key:
        :return: True/False for isSuccess
        '''
        body = {
                "deviceKeys": {
                                "appKey": app_key,
                                "devEUI": dev_eui,
                                "genAppKey": gen_app_key,
                                "nwkKey": nwk_key
                                }
                }

        logger.info("adding key to device: " + dev_eui + " - " + app_key)

        try:
            r = requests.post(self._url('/devices/{:s}/keys'.format(dev_eui)), headers=self._header(), json=body)
            r.raise_for_status()
            return True

        except:
            logger.error('[ERROR] add_device_keys [code]: ' + str(r.status_code))
            logger.error("device_eui=" + dev_eui + ": " + r.json()["message"])
            return False

    def add_device_activation(self, devEUI, devAddress, app_key, nwk_key):
        '''
        This function is used for adding activation for ABP devices
        :param devEUI:
        :param devAddress:
        :param app_key:
        :param nwk_key:
        :return: True/False for isSuccess
        '''
        body = {
            "deviceActivation": {
                "aFCntDown": 0,
                "appSKey": app_key,
                "devAddr": devAddress,
                "devEUI": devEUI,
                "fCntUp": 0,
                "fNwkSIntKey": nwk_key,
                "nFCntDown": 0,
                "nwkSEncKey": nwk_key,
                "sNwkSIntKey": nwk_key
            }
        }

        logger.info("activating device: " + devEUI + " - " + devAddress)

        try:
            r = requests.post(self._url('/devices/{:s}/activate'.format(devEUI)), headers=self._header(), json=body)
            r.raise_for_status()
            return True

        except:
            logger.error('[ERROR] device activation [code]: ' + str(r.status_code))
            logger.error("device_eui=" + devEUI + ": " + r.json()["message"])
            return False

    # Given a dictionary (key=dev_EUI, value=dev_address), app key, and nwk key,
    # Activate all devices
    # assume all devices exists
    # assume all device share the same app and network key
    def add_devices_activation(self, dict_, app_key, nwk_key):
        r = 1

        for devEUI, devAddress in sorted(dict_.items()):
            logger.info('Adding activation for: %s' % devEUI)
            r &= self.add_device_activation(devEUI, devAddress, app_key, nwk_key)

            response = self.get_activation_by_devEUI(devEUI)

            if ((response['deviceActivation']['appSKey'] == app_key) &
                    (response['deviceActivation']['nwkSEncKey'] == nwk_key)):
                logger.info('Activation verified ')
            else:
                r &= 0

        if not r:
            logger.warning("Add device activation batch mode failed")

        return r

    def update_activation_by_application(self, app_id, app_key, nwk_key):
        logger.info('Getting devices for Application #%s', app_id)
        devices = self.get_devices_by_application(app_id)

        logger.info('Get activation')
        logger.info('DEV EUI          | APPLICATION_KEY                  NETWORK_KEY')

        for devEUI, devAdrs in sorted(devices.items()):

            r = self.get_activation_by_devEUI(devEUI)

            if not r:
                logger.info('Get activation failed for %s' % devEUI)
                logger.info('Add activation')

                r = self.add_device_activation(devEUI, devAdrs, app_key, nwk_key)

                time.sleep(0.1)
                if not r:
                    logger.warning('Unable to add activation for %s' % devEUI)
                    continue

                r = self.get_activation_by_devEUI(devEUI)

            logger.info('%s | %s %s' % (devEUI,
                                        r['deviceActivation']['appSKey'],
                                        r['deviceActivation']['nwkSEncKey']))
        return 1

    def enqueue_downlink_to_device(self, dev_eui, fPort, b64_payload, confirmed):
        logger.info('Queueing up downlink to device %s',dev_eui)

        try:
            assert (len(dev_eui) == 16)
            assert (is_hex(dev_eui))

            fPort = int(fPort)
            assert (fPort>=1 and fPort <=223)
            assert (isinstance(confirmed,bool))

        except AssertionError as e:
            logger.error('Assertion error on dev_eui: %s' % (dev_eui))
            return False

        body = {
            "deviceQueueItem": {
                "confirmed": confirmed,
                "data": b64_payload,
                "fPort": fPort,
            }
        }
        try:
            r = requests.post(self._url('/devices/{:s}/queue'.format(dev_eui)), headers=self._header(), json=body)
            r.raise_for_status()
            return True

        except:
            logger.error('[ERROR] queueing downlink [code]: ' + str(r.status_code))
            logger.error("dev_eui=" + dev_eui + ": " + r.json()["message"])
            return False

    def get_devices_by_application_with_last_seen(self, app_id):

        cnt = int(self.get_devices_count_by_application(app_id))
        set = []

        if cnt < 1:
            logger.warning("No device found in application %s" % str(app_id))

        for i in range(cnt):
            p = {"limit": "1", "offset": str(i), "applicationID": str(app_id)}

            val = requests.get(self._url('/devices'), headers=self._header(), params=p).json()["result"][0]
            set.append([val["devEUI"],val["name"],val["lastSeenAt"]])
            # set[i][1] = val["name"]
            # set[i][2] = val["lastSeenAt"]

        return set