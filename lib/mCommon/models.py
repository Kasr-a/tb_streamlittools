from enum import IntEnum, Enum, auto
from datetime import datetime
from time import time
import logging
logger = logging.getLogger(__name__)

# Models in this file

# Enum for sensor type as defined by Node 2.1 payload structure
# Superseded by Node 2.2, Skyla packet structures
class ScarletSensorType(Enum):
    STATUS = '0'
    TEMPERATURE = '1'
    STATUS2 = '2'
    HUMIDITY = '3'
    PRESSURE = '4'
    GPS = 'G'


class SensorType(IntEnum):
    '''
    These values follows the bit value as defined in alfred DB tbl_sensor_type
    '''
    TEMPERATURE = 1
    PRESSURE = 2
    HUMIDITY = 4
    WIND_SPEED = 8

    # Int values not yet defined in alfred DB
    BAROMETRIC = 2
    ANEMOMETER = 8
    WIND_DIRECTION = auto()
    SOUND = auto()
    LIGHT = auto()

    ACCELEROMETER = auto()
    GYROSCOPE = auto()
    MAGNETOMETER = auto()
    VOLTAGE = auto()

    ASCII = auto()

    CARBON_MONOXIDE     = auto()
    NITROGEN_DIOXIDE    = auto()
    AMMONIA             = auto()
    PROPANE             = auto()
    BUTANE              = auto()
    METHANE             = auto()
    HYDROGEN            = auto()
    ETHANOL             = auto()
    IAQ                 = auto()
    VOCC                = auto()
    GAS_R               = auto()           # todo: to be moved to subpacket.py

    GPS                 = auto()
    DOOR_SENSOR         = auto()
    LIQUID_LEVEL        = auto()
    LEAK                = auto()
    FIRE_EXTINGUISHER   = auto()
    CT                  = auto()
    OIL_DEBRIS          = auto()
    LIGHTNING           = auto()

    GAS_FLAG            = auto()           # todo: to be moved to subpacket.py
    CHARGER_STATUS      = auto()            # todo: to be moved to subpacket.py

    HYDROGEN_SULFIDE    = auto()
    CARBON_DIOXIDE      = auto()
    VIBRATION           = auto()


SensorTypeEnumToShortStr = {
    SensorType.TEMPERATURE:         "T",
    SensorType.HUMIDITY:            "H",
    SensorType.PRESSURE:            "P",
    SensorType.WIND_SPEED:          'WS',
    SensorType.SOUND:               'S',
    SensorType.CARBON_MONOXIDE:     'CO',
    SensorType.NITROGEN_DIOXIDE:    'NO2',
    SensorType.AMMONIA:             'NH3',
    SensorType.PROPANE:             'C3H8',
    SensorType.BUTANE:              'C4H10',
    SensorType.METHANE:             'CH4',
    SensorType.HYDROGEN:            'H2',
    SensorType.ETHANOL:             'C2H5OH',
    SensorType.IAQ:                 'IAQ',
    SensorType.GAS_R:               'GR',

    SensorType.DOOR_SENSOR:         'DS',
    SensorType.LIQUID_LEVEL:        'LL',
    SensorType.LEAK:                'L',
    SensorType.FIRE_EXTINGUISHER:   'FE',
    SensorType.CT:                  'CT',
    SensorType.OIL_DEBRIS:          'OD',
    SensorType.LIGHTNING:           'LG',

    SensorType.GAS_FLAG:            'GF',
    SensorType.CHARGER_STATUS:      'CS',

    SensorType.HYDROGEN_SULFIDE:    'HS',
    SensorType.CARBON_DIOXIDE:      'CO2',
    SensorType.VIBRATION:           'VN'

}

SensorTypeStrToEnum = {
    "Temperature":          SensorType.TEMPERATURE,
    "Humidity":             SensorType.HUMIDITY,
    "Pressure":             SensorType.PRESSURE,
    'Wind Speed':           SensorType.WIND_SPEED,
    'Sound':                SensorType.SOUND,
    'Carbon Monoxide':      SensorType.CARBON_MONOXIDE,
    'Nitrogen Dioxide':     SensorType.NITROGEN_DIOXIDE,
    'Ammonia':              SensorType.AMMONIA,
    'Propane':              SensorType.PROPANE,
    'Butane':               SensorType.BUTANE,
    'Methane':              SensorType.METHANE,
    'Hydrogen':             SensorType.HYDROGEN,
    'Ethanol':              SensorType.ETHANOL,
    'IAQ':                  SensorType.IAQ,
    'Gas Resistance':       SensorType.GAS_R,

    'Door Sensor':          SensorType.DOOR_SENSOR,
    'Liquid Level':         SensorType.LIQUID_LEVEL,
    'Leak Detection':       SensorType.LEAK,
    'Fire Extinguisher':    SensorType.FIRE_EXTINGUISHER,
    'CT':                   SensorType.CT,
    'Oil Debris':           SensorType.OIL_DEBRIS,
    'Lightning':            SensorType.LIGHTNING,

    'Carbon Dioxide':       SensorType.CARBON_DIOXIDE,
    'Hydrogen Sulfide':     SensorType.HYDROGEN_SULFIDE,
    'Vibration':            SensorType.VIBRATION

}
SensorTypeEnumToStr = {
    SensorType.TEMPERATURE:         "Temperature",
    SensorType.HUMIDITY:            "Humidity",
    SensorType.PRESSURE:            "Pressure",
    SensorType.WIND_SPEED:          'Wind Speed',
    SensorType.SOUND:               'Sound',
    SensorType.CARBON_MONOXIDE:     'Carbon Monoxide',
    SensorType.NITROGEN_DIOXIDE:    'Nitrogen Dioxide',
    SensorType.AMMONIA:             'Ammonia',
    SensorType.PROPANE:             'Propane',
    SensorType.BUTANE:              'Butane',
    SensorType.METHANE:             'Methane',
    SensorType.HYDROGEN:            'Hydrogen',
    SensorType.ETHANOL:             'Ethanol',
    SensorType.IAQ:                 'IAQ',
    SensorType.GAS_R:               'Gas Resistance',

    SensorType.DOOR_SENSOR:         'Door Sensor',
    SensorType.LIQUID_LEVEL:        'Liquid Level',
    SensorType.LEAK:                'Leak Detection',
    SensorType.FIRE_EXTINGUISHER:   'Fire Extinguisher',
    SensorType.CT:                  'CT',
    SensorType.OIL_DEBRIS:          'Oil Debris',
    SensorType.LIGHTNING:           'Lightning',

    SensorType.GAS_FLAG:            'Gas Flag',
    SensorType.CHARGER_STATUS:      'Charger Status',

    SensorType.CARBON_DIOXIDE:      'Carbon Dioxide',
    SensorType.HYDROGEN_SULFIDE:    'Hydrogen Sulfide',
    SensorType.VIBRATION:           'Vibration'
}


def get_SensorTypeEnum_by_ShortStr(short_str: str):
    for k, v in SensorTypeEnumToShortStr.items():
        if v == short_str:
            return k

    return None

# the name here must match the sensor types as defined in the DB


class Sensor(object):
    def __init__(self, id, t, channel_id, alfred_db, uid=0, metadata_db=None):
        self.id = id
        self.uid = uid
        self.type = t
        self.alfred_db = alfred_db
        self.channel_id = channel_id
        self.measurement = 0
        self.metadata_db = metadata_db

    def write_measurement_data(self):
        if self.metadata_db:
            logger.debug('working on this')
            return 1
        else:
            return self.alfred_db.sp_insert_measurement_datapoint(self.channel_id, self.id, self.measurement)

    def get_data_bundle(self, timestamp=0):
        ts = round(time())*1000 if not timestamp else timestamp
        r = {
                "channelId": self.channel_id,
                "sensorId": self.id,
                "data": self.measurement,
                "timestamp": ts
            }
        return r


class Channel(object):

    def __init__(self, ch_id, serial_number, isActive, status, alfred_db):
        self.id = ch_id
        self.serial_number = serial_number
        self.isActive = isActive
        self.status = status
        self.alfred_db = alfred_db

        self.n_sensor = 0

        # all sensors within a channel are assumed to be of the same type
        self.keyed_sensors = {}

    def get_all_sensors_id(self):
        return list(s.id for s in self.sensors)

    def get_all_sensors_uid(self):
        return list(s.uid for s in self.sensors)

    def get_all_sensors_type(self):
        return list(s.type for s in self.sensors)

    def get_all_sensors_wavelength(self):
        return list(s.peak.wavelength for s in self.sensors)

    def get_all_sensors_measurement(self):
        return list(s.measurement for s in self.sensors)

    def write_single_measurement_data(self, s_id, val):
        try:
            s = self.keyed_sensors[s_id]
        except Exception as e:
            logger.error("access keyed sensor failed - s_id=%d, error: %s" % (s_id, e))
            return 0

        try:
            s.measurement = val
            logger.debug("writing measurement to ch%02d-s%02d: %03.1f" % (self.id, s_id, s.measurement))
            s.write_measurement_data()

        except Exception as e:
            logger.error("write measurement error - s_id=%d, error: %s" % (s_id, e))
            return 0

        return 1

    def get_single_sensor_data_bundle(self, s_id, val, timestamp=0):

        ts = round(time()) * 1000 if not timestamp else timestamp

        try:
            s = self.keyed_sensors[s_id]
        except Exception as e:
            logger.error("access keyed sensor failed - s_id=%d, error: %s" % (s_id, e))
            return {}

        try:
            s.measurement = val
            logger.debug("get_single_sensor_data_bundle ch%02d-s%02d: %03.1f (only 1 decimal shown)" % (self.id, s_id, s.measurement))
            return s.get_data_bundle(ts)

        except Exception as e:
            logger.error("get_single_sensor_data_bundle error - s_id=%d, error: %s" % (s_id, e))
            return {}

