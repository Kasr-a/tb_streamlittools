# API Endpoints for Colton Data Service and Cognito Authentication.
# Move to environment variable if needed.
# Staging
import http
from datetime import datetime, timedelta
from typing import Optional

import requests

from ..config import *

logger = logging.getLogger(__name__)

COLTON_DATA_SVC_URL = COLTON_DATA_HOST
COLTON_INSERT_SENSOR_ENDPOINT = f'{COLTON_DATA_SVC_URL}/mw/insert/sensors'
COLTON_INSERT_LOCATION_ENDPOINT = f'{COLTON_DATA_SVC_URL}/location/insert'
COLTON_INSERT_DEVICE_ENDPOINT = f'{COLTON_DATA_SVC_URL}/device/insert'

COGNITO_AUTH_SVC_URL = COGNITO_AUTH_HOST
COGNITO_AUTH_ENDPOINT = f'{COGNITO_AUTH_SVC_URL}/user/token'
COGNITO_REFRESH_ENDPOINT = f'{COGNITO_AUTH_SVC_URL}/user/refresh_token'


class ColtonDataService(object):
    """
    Singleton Service class to access Colton Data Service.
    """
    __instance__ = None

    _username: str
    _password: str
    _user_pool: str

    access_token: Optional[str]
    refresh_token: Optional[str]
    acc_tok_exp_date: Optional[datetime]
    ref_tok_exp_date: Optional[datetime]

    @staticmethod
    def get_instance(username: str = None, password: str = None, user_pool: str = 'aoms'):
        """ Static access method. """
        if ColtonDataService.__instance__ is None:
            ColtonDataService(username, password, user_pool)
        elif username and password:
            ColtonDataService._username = username
            ColtonDataService._password = password
            ColtonDataService._user_pool = user_pool
            ColtonDataService.get_access_token(ColtonDataService.__instance__)
        return ColtonDataService.__instance__

    def __init__(self, username: str, password: str, user_pool: str):
        """ Virtually private constructor. """
        if ColtonDataService.__instance__ is not None:
            raise Exception("This class is a singleton!")
        else:
            if username and password:
                self._username = username
                self._password = password
                self._user_pool = user_pool
                self.access_token = None
                self.refresh_token = None
                self.acc_tok_exp_date = None
                self.ref_tok_exp_date = None
                self.get_access_token()
                ColtonDataService.__instance__ = self
            else:
                raise CognitoException("Username and password should be provided for initialize service instance.")

    def get_access_token(self):

        if self.access_token and self.acc_tok_exp_date > datetime.utcnow():
            # Already authenticated, directly using existing access token.
            return self.access_token
        elif self.refresh_token and self.ref_tok_exp_date > datetime.utcnow():
            # Already authenticated but access token maybe expired, try using refresh token
            # to get new access token.
            auth_header = {
                "Authorization": f'Bearer {self.access_token}'
            }

            refresh_request = {
                "pool": self._user_pool,
                "refreshToken": self.refresh_token
            }

            refresh_response = requests.post(COGNITO_REFRESH_ENDPOINT, headers = auth_header, json = refresh_request)

            if refresh_response.status_code != http.HTTPStatus.OK:
                raise CognitoException(f'Fail to refresh access token, Error Message:{refresh_response.text}')
            else:
                refresh_result = refresh_response.json()
                # For safety, minus 30 seconds from expired time.
                expire_in = refresh_result.get("AuthenticationResult").get('ExpiresIn') - 30
                expire_in = refresh_result.get("AuthenticationResult").get('ExpiresIn') - 30
                self.access_token = refresh_result.get("AuthenticationResult").get("AccessToken")
                self.acc_tok_exp_date = datetime.utcnow() + timedelta(seconds = expire_in)
                return self.access_token

        else:
            # First time authenticate.
            auth_request = {
                "username": self._username,
                "password": self._password
            }

            auth_response = requests.post(COGNITO_AUTH_ENDPOINT, json = auth_request)
            if auth_response.status_code != http.HTTPStatus.OK:
                raise CognitoException(f'Fail to get access token, Error Message:{auth_response.text}')
            else:
                auth_result = auth_response.json()
                # For safety, minus 30 seconds from expired time.
                expire_in = auth_result.get("AuthenticationResult").get('ExpiresIn') - 30
                self.access_token = auth_result.get("AuthenticationResult").get("AccessToken")
                self.refresh_token = auth_result.get("AuthenticationResult").get("RefreshToken")
                self.acc_tok_exp_date = datetime.utcnow() + timedelta(seconds = expire_in)
                self.ref_tok_exp_date = datetime.utcnow() + timedelta(days = 29)
                return self.access_token

    def build_auth_header(self):
        access_token = self.get_access_token()
        return {
            "Authorization": f'Bearer {access_token}'
        }

    def insert_sensor_data(self, data: dict):
        headers = self.build_auth_header()
        logger.debug(headers)
        insert_response = requests.post(COLTON_INSERT_SENSOR_ENDPOINT, headers = headers, json = data)
        if insert_response.status_code != http.HTTPStatus.CREATED:
            raise ColtonException(f'Fail to insert measurement data, Error detail: {insert_response.text}')
        else:
            return 0

    def insert_location_data(self, data: dict):
        headers = self.build_auth_header()
        logger.debug(headers)
        insert_response = requests.post(COLTON_INSERT_LOCATION_ENDPOINT, headers = headers, json = data)
        if insert_response.status_code != http.HTTPStatus.CREATED:
            raise ColtonException(f'Fail to insert location data, Error detail: {insert_response.text}')
        else:
            return 0

    def insert_device_data(self, data: dict):
        headers = self.build_auth_header()
        logger.debug(headers)
        insert_response = requests.post(COLTON_INSERT_DEVICE_ENDPOINT, headers = headers, json = data)
        if insert_response.status_code != http.HTTPStatus.CREATED:
            raise ColtonException(f'Fail to insert device data, Error detail: {insert_response.text}')
        else:
            return 0


class ColtonException(Exception):
    pass


class CognitoException(Exception):
    pass
