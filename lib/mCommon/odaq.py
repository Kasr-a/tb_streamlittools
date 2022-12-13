import lib.config as config

import logging
logger = logging.getLogger(__name__)

# Style guide
# module_name, package_name, ClassName, method_name, ExceptionName,
# function_name, GLOBAL_CONSTANT_NAME, global_var_name, instance_var_name,
# function_parameter_name, local_var_name

class States(object):
    comm_init = 1
    load_config = 2
    load_general_settings = 3
    channel_switch = 4
    diag_mode = 5

# Enum for sensor type
class SensorType(object):
    null = -1
    temperature = 1
    pressure = 4
    humidity = 3
    status = 0
    status2 = 2


SensorTypeString = {
    -1: "null",
    1:"temperature",
    2:"pressure",
    3:"humidity"
}


class ChannelHealth(object):
    healthy = 3
    moderate = 2
    disconnected = 0
    error = -1


class SensorHealth(object):
    healthy = 3
    moderate = 2
    weak = 1
    disconnected = 0
    error = -1


class Peak(object):
    def __init__(self, lb, hb, w=0, r=0):
        self.wavelength = w
        self.reflectivity = r
        self.LBOUND = lb
        self.HBOUND = hb

    def set_wavelength(self, w):
        if (w > self.LBOUND) and (w < self.HBOUND):
            self.wavelength = w
            return 1
        else:
            self.wavelength = 0
            return 0


class Sensor(object):
    def __init__(self, id, t, channel_id, alfred_db, uid=0):
        self.id = id
        self.uid = uid
        self.type = t
        self.alfred_db = alfred_db
        self.channel_id = channel_id

        self.health = 0
        self.h_r = 0
        self.h_dr = 0
        self.spectrum_w = []
        self.spectrum_r = []

    def update_health(self):

        dr = max(self.spectrum_r) - min(self.spectrum_r)
        r = max(self.spectrum_r)

        if dr > config.HEALTH_DR_HIGH:
            h_dr = SensorHealth.healthy
        elif dr > config.HEALTH_DR_MEDIUM:
            h_dr = SensorHealth.moderate
        elif dr > config.HEALTH_DR_LOW:
            h_dr = SensorHealth.weak
        else:
            h_dr = SensorHealth.disconnected

        if r > config.HEALTH_R_HIGH:
            h_r = SensorHealth.healthy
        elif r > config.HEALTH_R_MEDIUM:
            h_r = SensorHealth.moderate
        elif r > config.HEALTH_R_LOW:
            h_r = SensorHealth.weak
        else:
            h_r = SensorHealth.disconnected

        self.h_dr = h_dr
        self.h_r = h_r

        self.health = min(h_dr, h_r)

        #self.echo_health()

    def echo_health(self):
        logger.debug("S#%02d DR:%d | R:%d = %d" % (self.id, self.h_dr, self.h_r, self.health))


class Sensor_T(Sensor):

    def __init__(self, id, channel_id, alfred_db, uid=0):
        Sensor.__init__(self, id, SensorType.temperature, channel_id, alfred_db, uid)
        self.LAMBDA_A = 0
        self.T0 = 0
        self.G1 = 0
        self.G2 = 0
        self.CONST = 0
        self.LAMBDA_B = 0
        self.OFFSET = 0

        self.peak = Peak(0, 0, 0, 0)

        self.measurement = 0.0

    def write_measurement_data(self):
        return self.alfred_db.sp_insert_measurement_datapoint(self.channel_id, self.id, self.measurement)

    def calc_measurement(self):
        w = self.peak.wavelength

        if w == 0:
            self.measurement = -99
        else:
            self.measurement = self.G1 * pow((w/self.LAMBDA_A-self.LAMBDA_B), 2) + \
                               self.G2 * (w/self.LAMBDA_A-self.LAMBDA_B) + \
                               self.CONST + self.T0 + self.OFFSET

    def get_calibration(self, ch_id):
        if ch_id < 1:
            return 0

        lst = self.alfred_db.sp_getCalibrationMTS(ch_id, self.id)

        try:
            lst = lst[0]
        except:
            logger.critical("CH#%02d-%02d Load temperature calibration "
                            "Error", ch_id, self.id)
            return 0

        if None in lst:
            logger.critical("CH#%02d-%02d Load temperature calibration "
                             "Error - One of the parameter is Null?", ch_id, self.id)
            return 0

        if len(lst) != 9:
            logger.critical("CH#%02d-%02d Temperature calibration list "
                             "does not match tbl_sensors - check ALFRED",
                             ch_id, self.id)
            return 0

        self.LAMBDA_A = float(lst[0])
        self.T0 = float(lst[1])
        self.G1 = float(lst[2])
        self.G2 = float(lst[3])
        self.CONST = float(lst[4])
        # self.LBOUND = lst[5]
        # self.HBOUND = lst[6]
        self.LAMBDA_B = float(lst[7])
        self.OFFSET = float(lst[8])

        self.peak = Peak(lb=float(lst[5]), hb=float(lst[6]))

        return 1

    def echo_calibration(self, ch_id):
        logger.debug("%02d-%02d - %6.2f %6.2f %6.2f %6.2f %6.2f %8.2f %8.2f %8.2f %6.2f" \
                      % (ch_id, self.id, self.LAMBDA_A, self.T0, self.G1, self.G2, \
                      self.CONST, self.peak.LBOUND, self.peak.HBOUND,self.LAMBDA_B, self.OFFSET))
        return 1

class Sensor_P(Sensor):
    def __init__(self, id, channel_id, alfred_db, uid=0):
        Sensor.__init__(self, id, SensorType.pressure, channel_id, alfred_db, uid)
        self.id = id
        self.LAMBDA_0_A = 0
        self.G2_A = 0
        self.G1_A = 0
        self.CONST_A = 0
        self.LAMBDA_0_B = 0
        self.G2_B = 0
        self.G1_B = 0
        self.CONST_B = 0

    def write_measurement_data(self):
        return self.alfred_db.sp_insert_measurement_datapoint(self.channel_id, self.id, self.measurement)

    # add p calibration handler
    def get_calibration(self, ch_id):
        return 1

    def echo_calibration(self, ch_id):
        return 1


class Sensor_H(Sensor):
    def __init__(self, id, channel_id, alfred_db, uid=0):
        Sensor.__init__(self, id, SensorType.humidity, channel_id, alfred_db, uid)
        self.peak = Peak(0, 0, 0, 0)
        self.measurement = 0.0

    def write_measurement_data(self):
        return self.alfred_db.sp_insert_measurement_datapoint(self.channel_id, self.id, self.measurement)


class Channel(object):

    def __init__(self, ch_id, serial_number, isActive, status, alfred_db):
        self.id = ch_id
        self.serial_number = serial_number  # todo: remove?
        self.isActive = isActive
        self.status = status
        self.health = ChannelHealth.disconnected

        self.WIDTH = 0
        self.NOISE_FLOOR = 0
        self.WIDTH_LVL = 0
        self.REL_THRESHOLD = 0
        self.LENGTH = 0
        self.MODE = 0
        self.MIN_PEAK_DIST = 0

        self.n_sensor = 0

        self.sensors = ()
        self.keyed_sensors = {}

        self.empty = 0          # indicating the lastest scan of this channel yielded no wavelength

        self.alfred_db = alfred_db

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

    def update_spectrum(self, w, dbm, w_start, w_delta):
        if self.n_sensor:
            for s in self.sensors:
                # parse the spectrum into sections for the sensor
                i_start = int((s.peak.LBOUND - w_start) / w_delta)
                i_end = int((s.peak.HBOUND - w_start) / w_delta)

                s.spectrum_w = w[i_start:i_end]
                s.spectrum_r = dbm[i_start:i_end]

            return 1
        else:
            return 0

    def echo_all_sensor_health(self):
        h = list(s.health for s in self.sensors)
        logger.debug("Sensor Health: %s", ','.join(list('%02d' % x for x in h)))

    def update_channel_health(self, dbm):

        # if there's sensors configured
        if self.n_sensor:

            for s in self.sensors:
                s.update_health()

            # if any(list(s.health == 0 for s in self.sensors)):
            #     return ChannelHealth.disconnected

            h = list(s.health for s in self.sensors)
            self.health = sum(h)/len(h)

        else:

            if bool(dbm):
                dr = max(dbm) - min(dbm)

                if dr > config.HEALTH_DR_MEDIUM:
                    self.health = ChannelHealth.healthy
                elif dr > config.HEALTH_DR_LOW:
                    self.health = ChannelHealth.moderate
                else:
                    self.health = ChannelHealth.disconnected
            else:
                self.health = ChannelHealth.error

        return 1

    def write_channel_health(self):
        try:
            self.alfred_db.sp_updateChannelHealth(self.id, config.health_state[self.health])

        except:
            logger.warning('Channel Health Write to Alfred Fail')
            return 0

        return 1

    def write_sensor_health(self):
        try:
            if self.n_sensor:
                for s in self.sensors:
                    self.alfred_db.sp_updateSensorHealth(self.id, s.id, config.health_state[s.health])

        except:
            logger.warning('Sensor Health Write to Alfred Fail - CH#%02d', self.id)
            return 0

        return 1

    def set_wavelengths(self, ws):

        # TODO: Error check needed here
        # TODO: what if multiple peaks are detection in the same bin?
        # assume ws is sorted in ascending order
        # assume the smallest item in ws is larger than sensors[0].LBOUND

        if len(ws) == 0:
            logger.debug("No wavelength available, setting all sensors to zero")
            for s in self.sensors:
                s.peak.wavelength = 0
            self.empty = 1
            return

        # if ws[0] < self.sensors[0].peak.LBOUND:
        #     logger.warning('First measured w = %8.3f < first expected w = %8.3f, set all w=0', \
        #                    ws[0], self.sensors[0].peak.LBOUND)
        #     for s in self.sensors:
        #         s.peak.wavelength = 0
        #     self.empty = 1
        #     return

        # algorithm for allocating each measured wavelength into sensors
        # assumes both the expected and measured wavelengths are provided in ascending order
        i = 0
        for w in ws:
            # logger.debug("i=%s w=%s" % (i, w))

            for j in range(i, self.n_sensor):
                if self.sensors[j].peak.set_wavelength(w):
                    i = j+1       # set the next search to start at the last sensor found location
                    break

        self.empty = 0

    def write_peak_data(self, timestamp):

        if self.empty:
            logger.debug("No peak info. Skipping write wavelength to Alfred")
            return

        self.alfred_db.sp_setSpectrum(self.id, timestamp)
        sp_id = self.alfred_db.sp_getSpectrumLargestID()

        ids = self.get_all_sensors_id()
        peaks = self.get_all_sensors_wavelength()

        self.alfred_db.sp_setWavelength(sp_id, self.id, ids, peaks, 1)

        return 1

    def write_measurement_data(self):

        if self.empty:
            logger.debug("No peak info. Skipping write measurement to Alfred")
            return

        type_string = SensorTypeString(self.sensors[0].type)
        data_set = zip(self.get_all_sensors_uid, self.get_all_sensors_measurement())

        self.alfred_db.sp_insert_measurement_datapoints(type_string, data_set)

    def calc_measurement(self):
        for s in self.sensors:
            s.calc_measurement()

    # todo: legacy
    def echo_wavelength(self):
        return '|'.join([' %08.3f' % (x.peak.wavelength) for x in self.sensors])

    def echo_measurement(self):
        return '|'.join([' %04.2f' % (x.measurement) for x in self.sensors])

    def echo_calibration(self):

        logger.info("Calibration for Channel %02d - N=%02d", self.id, self.n_sensor)
        for s in self.sensors:
            s.echo_calibration(self.id)

    def echo_setting(self):
        logger.info("Channel Settings for Channel %02d", self.id)
        logger.debug("%02d - %6.4f, %6.4f, %6.4f, %6.4f, %6.4f, %2f, %2d" % \
               (self.id, self.WIDTH, self.NOISE_FLOOR, self.WIDTH_LVL, \
                self.REL_THRESHOLD, self.LENGTH, self.MODE, self.MIN_PEAK_DIST))

    def get_calibration(self):

        # if self.n_sensor < 1:
        #     logger.warning('No sensor found in channel %2d', self.id)
        #     return 0
        w = 1

        try:
            for s in self.sensors:
                w &= s.get_calibration(self.id)
        except:
            logger.warning('Sensor Get Calibration Error')
            return 0

        return w

    # Talk to Alfred to get a copy of the sensors in each channel
    def populate(self):

        # get list of sensors from Alfred
        lst = self.alfred_db.sp_getSensorsByChannel(self.id)
        self.n_sensor = len(lst)

        # Error check
        if self.n_sensor < 1:
            logger.warning('No sensor found in channel %2d', self.id)
            return 0

        else:
            # build list of sensors into a tuple
            self.sensors = ()

            for i, type, uid in lst:
                if type == SensorType.temperature:
                    # todo: remove support for ch.sensors, use keyed sensors
                    self.sensors += (Sensor_T(i, self.id, self.alfred_db, uid),)
                    self.keyed_sensors[i] = Sensor_T(i, self.id, self.alfred_db, uid)
                    logger.debug("CH#%02d-%02d Added T Sensor", self.id, i)
                elif type == SensorType.pressure:
                    self.sensors += (Sensor_P(i, self.id, self.alfred_db, uid),)
                    self.keyed_sensors[i] = Sensor_P(i, self.id, self.alfred_db, uid)
                    logger.debug("CH#%02d-%02d Added P Sensor", self.id, i)
                elif type == SensorType.humidity:
                    self.sensors += (Sensor_H(i, self.id, self.alfred_db, uid),)
                    self.keyed_sensors[i] = Sensor_H(i, self.id, self.alfred_db, uid)
                    logger.debug("CH#%02d-%02d Added H Sensor", self.id, i)
                else:
                    logger.warning("Invalid sensor type: %s. Skip add" % type)

            return 1

    def get_channel_settings(self):

        try:
            lst = self.alfred_db.sp_getChannelSettings(self.id)[0]
        except Exception as e:
            logger.warning('WARNING: load channel setting fail - %s', e)
            return 0

        lst = [float(x) for x in lst]

        if len(lst) != 7:
            logger.warning("Incorrect/empty channel setting format!")

            # apply default channel settings
            self.WIDTH = 0.05
            self.NOISE_FLOOR = -50
            self.WIDTH_LVL = 3
            self.REL_THRESHOLD = -8
            self.LENGTH = 0
            self.MODE = 1
            self.MIN_PEAK_DIST=50

            return 0

        self.WIDTH = lst[0]
        self.NOISE_FLOOR = lst[1]
        self.WIDTH_LVL = lst[2]
        self.REL_THRESHOLD = lst[3]
        self.LENGTH = lst[4]
        self.MODE = int(lst[5])
        self.MIN_PEAK_DIST = int(lst[6])

        return 1


class Odaq(object):

    def __init__(self, alfred_db):
        logger.info("Initializing ODAQ object")
        self.n_ch = 0;
        self.i_ch = 0;  # current index for channel switching
        self.ch = []  # no need?

        # self.lst_channel = []
        self.lst_spectrum = []

        # Environment readings
        self.env = ""
        self.ready = ''

        # Settings
        self.INTERVAL_CH_SCAN = 1000
        self.POST_SWITCH_WAIT_TIME = 750
        self.PEAK_DETECTION_MODE = 1
        self.INTERVAL_ENVIRONMENT_DATA = 1
        self.INTERVAL_MEASURED_DATA = 1
        self.SPECTRUM_FILE_NAME = ""
        self.SPECTRUM_PATH = ""
        self.INTERVAL_SPECTRUM_SAVE = 60
        self.DETECTION_SETTING_ID = 128
        self.INIT_RETRY_DELAY = 30

        self.FLAG_CHANNEL_UPDATE = 0
        self.FLAG_SENSOR_UPDATE = 0
        self.FLAG_GENERAL_SETTINGS_UPDATE = 0
        self.FLAG_IS_CH_DIAG = 0

        # control variable to indicate that Odaq
        # just booted up for the first time
        self.is_first_time = 1

        self.ts_channel = 0  # timestamp for channel
        self.ts_cycle = 0    # timestamp for scan cycle

        self.spectrum_logger_index = 0

        self.state = 0

        self.alfred_db = alfred_db

        # self.tenant_id = tenant_id
        # self.db_config = db_config
        # self.init_mysql()


    # def init_mysql(self):
    #     return self.alfred_db.init(self.tenant_id, self.db_config, self.FLAG_IS_CH_DIAG)


    def set_alfred_db(self, alfred_db):
        self.alfred_db = alfred_db
        for channel in self.ch:
            channel.alfred_db = alfred_db
            for sensor in channel.sensors:
                sensor.alfred_db = alfred_db

        return 1

    # this function is deprecated
    def ping_alfred(self):
        return self.alfred_db.init_ping()

    # Run SQL statement to Alfred to get a list of channels
    # Returns 0 if no channel is found

    def create_channels(self):

        # get list of channels from Alfred
        lst = self.alfred_db.sp_getChannels()
        self.n_ch = len(lst)

        if len(lst) < 1:
            logger.info('No channel found')
            return 0

        self.ch = [Channel(i, str(sn), ia, st, self.alfred_db) \
                   for i, sn, ia, st in lst]

        for i, sn, ia, st in lst:
            logger.info('Created Channel #%02d - %10s', i, sn)

        return 1

    def write_env_data(self, lst):

        if lst == 0:
            logger.info("There's nothing to write to Alfred")
            return 0

        try:
            self.alfred_db.writeEnvironSensorMeasurement(lst)
        except:
            logger.critical("write env reading to Alfred failed")
            return 0

        return 1

    def get_internal_settings(self):

        try:
            lst = self.alfred_db.sp_getAllInternalSetting()

            d = {str(key): int(value) for (key, value) in lst}

            self.FLAG_CHANNEL_UPDATE = int(d['FLAG_CHANNEL_UPDATE'])
            self.FLAG_GENERAL_SETTINGS_UPDATE = int(d['FLAG_GENERAL_SETTINGS_UPDATE'])
            self.FLAG_IS_CH_DIAG = int(d['FLAG_IS_CH_DIAG'])

            return d

        except:
            logger.critical("Get Internal Setting(s) Error/Fail!")

    def get_general_settings(self):

        try:
            lst = self.alfred_db.sp_getAllGeneralSetting()

            d = {str(key): value for (key, value) in lst}

            self.INTERVAL_CH_SCAN = int(d['INTERVAL_CH_SCAN'])
            self.INTERVAL_ENVIRONMENT_DATA = int(d['INTERVAL_ENVIRONMENT_DATA'])
            self.INTERVAL_MEASURED_DATA = int(d['INTERVAL_MEASURED_DATA'])
            self.INTERVAL_SPECTRUM_SAVE = int(d['INTERVAL_SPECTRUM_SAVE'])
            self.PEAK_DETECTION_MODE = int(d['PEAK_DETECTION_MODE'])
            self.POST_SWITCH_WAIT_TIME = int(d['POST_SWITCH_WAIT_TIME'])
            self.SPECTRUM_FILE_NAME = d['SPECTRUM_FILE_NAME']
            self.SPECTRUM_PATH = d['SPECTRUM_PATH']
            self.DETECTION_SETTING_ID = int(d['DETECTION_SETTING_ID'])

            return d

        except:
            logger.critical("Get General Setting(s) fail!")
            return 0

    def set_flag_general_settings_update(self, val):
        self.FLAG_GENERAL_SETTINGS_UPDATE = val
        self.alfred_db.sp_setInternalSetting('FLAG_GENERAL_SETTINGS_UPDATE', val)

    def set_flag_channel_update(self, val):
        self.FLAG_CHANNEL_UPDATE = val
        self.alfred_db.sp_setInternalSetting('FLAG_CHANNEL_UPDATE', val)

    # todo: legacy
    def check_channel_update(self):
        return int(self.alfred_db.sp_getInternalSetting
                   ("FLAG_CHANNEL_UPDATE"))

    # todo: legacy
    def check_sensor_update(self):
        return int(self.alfred_db.sp_getInternalSetting
                   ("FLAG_SENSOR_UPDATE"))

    # todo: legacy
    def check_general_settings_update(self):
        return int(self.alfred_db.sp_getInternalSetting
                   ("FLAG_GENERAL_SETTINGS_UPDATE"))

    # todo: legacy
    def send_heart_beat(self):
        self.alfred_db.sp_setHeartBeat()
