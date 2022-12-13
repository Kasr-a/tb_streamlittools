import logging

import mysql.connector
import pandas as pd
from mysql.connector.conversion import MySQLConverter

logger = logging.getLogger(__name__)


class Alfred(object):

    def __init__(self, tenant_id, db_config, diag_mode):
        self._tenant_id = tenant_id
        self._db_config = db_config
        self._diagnostic_mode = diag_mode

    # todo: legacy
    def set_diagnostic_mode(self, var):
        self._diagnostic_mode = var

    # todo: legacy
    def get_diagnostic_mode(self):
        return self._diagnostic_mode

    # todo: legacy
    def set_tenant_id(self, var):
        self._tenant_id = var

    # todo: legacy
    def get_tenant_id(self):
        return self._tenant_id

    # todo: legacy
    def set_db_config(self, var):
        self._db_config = var

    # todo: legacy
    def get_db_config(self):
        return self._db_config

    def init_ping(self):
        cnx = None
        try:
            logger.debug(f'init_ping: {self._db_config}')
            cnx = mysql.connector.connect(**self._db_config)
            logger.info("Established Alfred MySQL connection")

            return 1
        except:
            logger.warning("Alfred MySQL connection failed!")
            return 0
        finally:
            if cnx:
                cnx.close()

    # TODO: change this to drop table
    def init_fresh_start(self, table_names):

        logger.debug(self._db_config)
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()

            if table_names is None:
                return

            for tn in table_names:
                query = "DELETE FROM aoms." + tn + ";"
                cursor.execute(query);
                # print 'Deleted all from ' + tn

            cnx.commit()
            cursor.close()
            return 1
        finally:
            cnx.close()

    def init_channel_settings(self, ch, _w, _nf, _wl, _rt, _l):

        logger.debug(self._db_config)
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()

            for i, w, nf, wl, rt, l in zip(ch, _w, _nf, _wl, _rt, _l):
                query = "INSERT INTO aoms.tbl_channelsettings " \
                        "(TENANT, CHANNEL, WIDTH, NOISE_FLOOR, WIDTH_LVL, REL_THRESHOLD, LENGTH) " \
                        "SELECT %s, c.id, %s, %s, %s, %s, %s " \
                        "FROM tbl_channel as c " \
                        "WHERE c.channel_id = %s and c.isActive = 1 and not c.isDiagnostic;"
                cursor.execute(query, (self._tenant_id, w, nf, wl, rt, l, i))

            cnx.commit()
            cursor.close()
        finally:
            cnx.close()

    # todo
    def init_calibration_mts(self, ids, chs, _lb1, _t0, _g1, _g2, _c, _min, _max, _lb2, _o):

        logger.debug(self._db_config)
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()

            for id, ch, lb1, t0, g1, g2, c, min, max, lb2, o in \
                    zip(ids, chs, _lb1, _t0, _g1, _g2, _c, _min, _max, _lb2, _o):
                # print id, ch, lb1, t0, g1, g2, c, min, max, lb2, o

                query = "INSERT INTO aoms.tbl_calibration_mts (sensor, LAMBDA_A, T0, " \
                        "G1, G2, CONST, LBOUND, HBOUND, LAMBDA_B, OFFSET) " \
                        "SELECT s.id, %s, %s, %s, %s, %s, %s, %s, %s, %s " \
                        "FROM aoms.tbl_sensor as s INNER JOIN aoms.tbl_channel " \
                        "as c on c.id = s.channel " \
                        "WHERE c.channel_id = %s and c.isActive and not c.isDiagnostic " \
                        "and s.sensor_id = %s and s.isActive and not s.isDiagnostic;"
                cursor.execute(query, (lb1, t0, g1, g2, c, min, max, lb2, o, ch, id))

            cnx.commit()
            cursor.close()

            return 1
        finally:
            cnx.close()

    def init_insertSensors(self, ids, type, channel, isActive):

        logger.debug(self._db_config)
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            for i, t, ch, ia in zip(ids, type, channel, isActive):
                # print i, t, ch, ia
                query = ("INSERT INTO aoms.tbl_sensor (tenant, sensor_id, type, channel, isActive) "
                         "SELECT %s, %s, %s, c.id, %s from aoms.tbl_channel as c "
                         "WHERE c.channel_id = %s and c.isActive and not c.isDiagnostic;")
                cursor.execute(query, (self._tenant_id, i, t, ia, ch))
            cnx.commit()
            cursor.close()
        finally:
            cnx.close()

    def init_insertChannel(self, channel_id, sn, isActive):

        logger.debug(f'init_insertChannel: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = (
                "INSERT INTO aoms.tbl_channel (tenant, channel_id, serial_number, isActive) VALUES (%s, %s, %s, %s);")
            cursor.execute(query, (self._tenant_id, channel_id, str(sn), isActive))
            cnx.commit()
            cursor.close()
        finally:
            cnx.close()

    def sp_setHeartBeat(self):
        logger.debug(f'sp_setHeartBeat: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:

            cursor = cnx.cursor()
            r = cursor.callproc("sp_update_keepalive_mia", (self._tenant_id,))
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.warning('MySQL Error: %s', e)
        finally:
            cnx.close()

    def sp_setAllChannelInactive(self):

        logger.debug(f'sp_setAllChannelInactive: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = "UPDATE aoms.tbl_channel SET isActive=NULL WHERE isActive=1 and isDiagnostic=%s and tenant=%s;"
            cursor.execute(query, (self._diagnostic_mode, self._tenant_id))
            cnx.commit()
            cursor.close()
        finally:
            cnx.close()

    def sp_setAllSensorInactive(self):

        logger.debug(f'sp_setAllSensorInactive: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = "UPDATE aoms.tbl_sensor SET isActive=NULL WHERE isActive=1 and isDiagnostic=%s and tenant=%s;"
            cursor.execute(query, (self._diagnostic_mode, self._tenant_id))
            cnx.commit()
            cursor.close()
        finally:
            cnx.close()

    # Given a list of parameters read from Seb, enter readings into Alfred
    # All ID here are hardcoded
    # TODO: future improvement needed
    # todo
    def writeEnvironSensorMeasurement(self, lst):

        logger.debug(f'writeEnvironSensorMeasurement: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()

            for i, x in enumerate(lst):
                if x == 'nan':
                    lst[i] = -1

            # for these stored procedure, provide the sensor id
            args = (1, 'Temperature', lst[1])
            r = cursor.callproc("sp_insert_environ_datapoint", args)

            args = (2, 'Temperature', lst[2])
            r = cursor.callproc("sp_insert_environ_datapoint", args)

            args = (3, 'Temperature', lst[3])
            r = cursor.callproc("sp_insert_environ_datapoint", args)

            args = (1, 'Humidity', lst[4])
            r = cursor.callproc("sp_insert_environ_datapoint", args)

            args = (2, 'Humidity', lst[5])
            r = cursor.callproc("sp_insert_environ_datapoint", args)

            args = (1, 'Pressure', lst[6])
            r = cursor.callproc("sp_insert_environ_datapoint", args)

            # IGNORE lst index 7 and 8

            # for these SQL, provide the sensor id using the primary key from
            # tbl_environ_sensor

            query = "INSERT INTO aoms.tbl_environ_fan_speed (sensor, speed) " \
                    "SELECT %s, %s;"
            cursor.execute(query, (9, lst[9]))
            cursor.execute(query, (10, lst[10]))

            query = "INSERT INTO aoms.tbl_environ_sensor_flag (sensor, flag) " \
                    "VALUES (%s, %s);"
            id = range(11, 16)

            for i, s in enumerate(lst[11:]):
                cursor.execute(query, (id[i], int(s)))

            cnx.commit()

            cursor.close()
        finally:
            cnx.close()

    # todo: legacy
    def sp_getCalibrationLPS(self, channel_id, sensor_id):
        logger.debug(f'sp_getCalibrationLPS: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("SELECT lps.LAMBDA_0_A, lps.G2_A, lps.G1_A, lps.CONST_A, lps.LBOUND_A, lps.HBOUND_A, "
                     "lps.LAMBDA_0_B, lps.G2_B, lps.G1_B, lps.CONST_B, lps.LBOUND_B, lps.HBOUND_B "
                     "FROM aoms.tbl_calibration_lps AS lps "
                     "INNER JOIN aoms.tbl_sensor AS s ON s.id = lps.sensor "
                     "INNER JOIN aoms.tbl_channel AS c ON c.id = s.channel "
                     "WHERE c.channel_id = %s and c.isActive AND s.sensor_id = %s and s.isActive "
                     "and c.isDiagnostic = %s and s.isDiagnostic = %s;")
            cursor.execute(query, (channel_id, sensor_id, self._diagnostic_mode, self._diagnostic_mode))
            result = cursor.fetchall()
            cursor.close()
            return result
        finally:
            cnx.close()

    # todo
    def sp_getCalibrationMTS(self, channel_id, sensor_id):
        logger.debug(f'sp_getCalibrationMTS: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("SELECT mts.LAMBDA_A, mts.T0, mts.G1, mts.G2, mts.CONST, "
                     "mts.LBOUND, mts.HBOUND, mts.LAMBDA_B, mts.OFFSET "
                     "FROM aoms.tbl_calibration_mts AS mts "
                     "INNER JOIN aoms.tbl_sensor AS s ON s.id = mts.sensor "
                     "INNER JOIN aoms.tbl_channel AS c ON c.id = s.channel "
                     "WHERE c.channel_id = %s and c.isActive AND s.sensor_id = %s and s.isActive "
                     "and c.isDiagnostic = %s and s.isDiagnostic = %s;")
            cursor.execute(query, (channel_id, sensor_id, self._diagnostic_mode, self._diagnostic_mode))
            result = cursor.fetchall()
            cursor.close()
            return result
        finally:
            cnx.close()

    def sp_getChannels(self):
        logger.debug(f'sp_getChannels: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("SELECT c.channel_id, c.serial_number, c.isActive, c.status "
                     "FROM aoms.tbl_channel AS c "
                     "WHERE c.isActive and isDiagnostic = %s and c.tenant = %s "
                     "ORDER BY c.channel_id ASC")
            cursor.execute(query, (self._diagnostic_mode, self._tenant_id))
            result = cursor.fetchall()
            cursor.close()
            return result
        finally:
            cnx.close()

    def sp_getChannelByID(self, channel_id):
        logger.debug(f'sp_getChannelByID: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("SELECT c.channel_id, c.serial_number, c.isActive, c.status "
                     "FROM aoms.tbl_channel AS c "
                     "WHERE c.isActive and isDiagnostic = %s and c.tenant = %s "
                     "and c.channel_id = %s "
                     "ORDER BY c.channel_id ASC")
            cursor.execute(query, (self._diagnostic_mode, self._tenant_id, channel_id))

            result = cursor.fetchall()
            cursor.close()
            return result
        finally:
            cnx.close()

    def sp_getSensorsByChannel(self, channel_id):
        logger.debug(f'sp_getSensorsByChannel: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("SELECT s.sensor_id, s.type, s.id "
                     "FROM aoms.tbl_sensor as s "
                     "INNER JOIN aoms.tbl_channel AS c ON c.id = s.channel "
                     "WHERE c.channel_id = %s and c.isActive AND s.isActive and "
                     "c.isDiagnostic = %s and s.isDiagnostic = %s and "
                     "c.tenant = %s and s.tenant = %s "
                     "order by s.sensor_id asc;")
            cursor.execute(query, (channel_id, self._diagnostic_mode, self._diagnostic_mode,
                                   self._tenant_id, self._tenant_id))
            result = cursor.fetchall()
            cursor.close()
            return result
        finally:
            cnx.close()

    # def sp_getSensorCountByChannel(channel_id):
    #     cnx = mysql.connector.connect(**self._db_config)
    #     cursor = cnx.cursor()
    #     query = ("SELECT SUM(CASE WHEN s.channel = %s THEN 1 ELSE 0 END) "
    #              "FROM aoms.tbl_sensor AS s")
    #     cursor.execute(query,(channel_id,))
    #     result = cursor.fetchall()
    #     cursor.close()
    #     cnx.close()
    #     return result

    # todo:
    def sp_getChannelSettings(self, channel_id):
        logger.debug(f'sp_getChannelSettings: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = (
                "SELECT cs.WIDTH, cs.NOISE_FLOOR, cs.WIDTH_LVL, cs.REL_THRESHOLD, cs.LENGTH, cs.MODE, cs.PLACE_HOLDER1 "
                "FROM aoms.tbl_channelsettings AS cs "
                "INNER JOIN aoms.tbl_channel AS c ON cs.channel = c.id "
                "WHERE c.channel_id = %s and c.isActive and c.isDiagnostic = %s "
                "and c.tenant = %s "
                "order by c.channel_id asc;")
            cursor.execute(query, (channel_id, self._diagnostic_mode, self._tenant_id))
            result = cursor.fetchall()
            cursor.close()
            return result
        finally:
            cnx.close()

    def sp_getAllGeneralSetting(self):
        logger.debug(f'sp_getAllGeneralSetting: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("SELECT gs.key, gs.val FROM aoms.tbl_generalsettings AS gs "
                     "WHERE gs.tenant = %s;")
            cursor.execute(query, (self._tenant_id,))
            result = cursor.fetchall()
            cursor.close()
            return result
        finally:
            cnx.close()

    def sp_getAllInternalSetting(self):
        logger.debug(f'sp_getAllInternalSetting: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("SELECT s.key, s.val FROM aoms.tbl_internalsettings AS s "
                     "WHERE s.tenant = %s;")
            cursor.execute(query, (self._tenant_id,))
            result = cursor.fetchall()
            cursor.close()
            return result

        finally:
            cnx.close()

    def sp_getGeneralSetting(self, setting_key):
        logger.debug(f'sp_getGeneralSetting: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("SELECT gs.val FROM aoms.tbl_generalsettings AS gs "
                     "WHERE gs.key = %s and gs.tenant = %s")
            cursor.execute(query, (setting_key, self._tenant_id))
            result = cursor.fetchone()
            cursor.close()
            return result[0]
        finally:
            cnx.close()

    def sp_setGeneralSetting(self, setting_key, setting_val):
        logger.debug(f'sp_setGeneralSetting: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("UPDATE aoms.tbl_generalsettings AS gs SET gs.val = %s "
                     "WHERE gs.key = %s and gs.tenant = %s;")
            cursor.execute(query, (setting_val, setting_key, self._tenant_id))
            cnx.commit()
            cursor.close()
        finally:
            cnx.close()

    def sp_getInternalSetting(self, setting_key):
        logger.debug(f'sp_getInternalSetting: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("SELECT gs.val FROM aoms.tbl_internalsettings AS gs "
                     "WHERE gs.key = %s and gs.tenant = %s")
            cursor.execute(query, (setting_key, self._tenant_id))
            result = cursor.fetchone()
            cursor.close()
            return result[0]
        finally:
            cnx.close()

    def sp_setInternalSetting(self, setting_key, setting_val):
        logger.debug(f'sp_setInternalSetting: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("UPDATE aoms.tbl_internalsettings AS gs SET gs.val = %s "
                     "WHERE gs.key = %s and gs.tenant = %s;")
            cursor.execute(query, (setting_val, setting_key, self._tenant_id))
            cnx.commit()
            cursor.close()
        finally:
            cnx.close()

    def sp_insert_node_rssi_snr(self, node_id, rssi, snr):
        logger.debug(f'sp_insert_node_rssi_snr: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (self._tenant_id, node_id, rssi, snr)
            r = cursor.callproc("sp_insert_node_rssi_snr_datapoint", args)
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.error('Insert node rssi/snr failed - %s' % e)
            return 0
        finally:
            cnx.close()

    def sp_insert_node_signal_strength(self, node_id, signal_strength, timestamp=None):
        try:
            logger.debug(f'sp_insert_node_signal_percentage_datapoint: {self._db_config}')
            cnx = mysql.connector.connect(**self._db_config)
            cursor = cnx.cursor()

            args = (self._tenant_id, node_id, signal_strength, timestamp)
            r = cursor.callproc("sp_insert_node_signal_percentage_datapoint", args)
            cnx.commit()
            cursor.close()

        except Exception as e:
            logger.error('sp_insert_node_signal_percentage_datapoint failed - %s' % e)
            return 0

        finally:
            cnx.close()

    def sp_insert_node_battery(self, node_id, percentage):
        logger.debug(f'sp_insert_node_battery: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (self._tenant_id, node_id, percentage)
            r = cursor.callproc("sp_insert_node_battery_datapoint", args)
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.error('Insert node battery failed - %s' % e)
            return 0
        finally:
            cnx.close()

    # todo: this function should be superceded by sp_insert_node_temperature_with_timestamp, see below
    def sp_insert_node_temperature(self, node_id, temperature):
        logger.debug(f'sp_insert_node_temperature: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (self._tenant_id, node_id, temperature)
            r = cursor.callproc("sp_insert_node_temperature_datapoint", args)
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.error('Insert node temperature failed - %s' % e)
            return 0
        finally:
            cnx.close()

    # todo: make ts with default value to supercede above function
    def sp_insert_node_temperature_with_timestamp(self, node_id, temperature, ts: str):
        try:
            logger.debug(f'sp_insert_node_temperature_with_timestamp: {self._db_config}')
            cnx = mysql.connector.connect(**self._db_config)
            cursor = cnx.cursor()

            args = (self._tenant_id, node_id, temperature, ts)
            r = cursor.callproc("sp_insert_node_temperature_datapoint_with_timestamp", args)
            cnx.commit()
            cursor.close()

        except Exception as e:
            logger.error('sp_insert_node_temperature_with_timestamp: failed - %s' % e)
            return 0

        finally:
            cnx.close()

    def sp_update_node_status(self, node_id: str, status: str, value: bool):
        logger.debug(f'sp_update_node_status: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (self._tenant_id, node_id, status, value)
            r = cursor.callproc("sp_update_node_status", args)
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.error('Update node status failed - %s' % e)
            return 0
        finally:
            cnx.close()

    def sp_updateChannelHealth(self, channel_id, val):
        logger.debug(f'sp_updateChannelHealth: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (self._tenant_id, channel_id, self._diagnostic_mode, val)
            r = cursor.callproc("sp_update_channel_health", args)
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.error('Update channel health failed - %s', e)
            return 0
        finally:
            cnx.close()

    def sp_updateSensorHealth(self, channel_id, sensor_id, val):
        logger.debug(f'sp_updateSensorHealth: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (self._tenant_id, channel_id, sensor_id, self._diagnostic_mode, val)
            r = cursor.callproc("sp_update_sensor_health", args)
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.error('Update sensor health failed - %s', e)
            return 0
        finally:
            cnx.close()

    # todo: legacy
    def sp_insert_environ_datapoint(self, sensor_id, type_string, val):
        logger.debug(f'sp_insert_environ_datapoint: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (self._tenant_id, sensor_id, type_string, val)
            r = cursor.callproc("sp_insert_environ_datapoint", args)
            cursor.close()
            return r
        except Exception as e:
            logger.error('Insert Environ Datapoint failed - %s', e)
            return 0
        finally:
            cnx.close()

    # Inputs:
    # type_string = "temperature", "humidity" etc'
    # data = list of (sensor_unique_id, value)
    def sp_insert_measurement_datapoints(self, type_string, data):
        logger.debug(f'sp_insert_measurement_datapoints: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            qry_insert_measurement = "INSERT INTO tbl_measurement_{0} (sensor, {0}) VALUES (%s, %s)".format(type_string)
            cursor.executemany(qry_insert_measurement, data)
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.error('Insert Measurment DatapointS failed - %s', e)
            return 0
        finally:
            cnx.close()

    # Insert one data point at a time using stored proc
    def sp_insert_measurement_datapoint(self, channel_id, sensor_id, val):
        logger.debug(f'sp_insert_measurement_datapoint: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            arg = (self._tenant_id, channel_id, sensor_id, val)
            r = cursor.callproc("sp_insert_measurement_datapoint", arg)
            cursor.close()
        except Exception as e:
            logger.error('Insert Measurement DatapointS failed - %s', e)
            return 0
        finally:
            cnx.close()


    # Insert one data point at a time using stored proc
    def sp_insert_measurement_datapoint_with_timestamp(self, channel_id, sensor_id, val, ts: str):
        try:
            logger.debug(f'sp_insert_measurement_datapoint_with_timestamp: {self._db_config}')
            cnx = mysql.connector.connect(**self._db_config)
            cursor = cnx.cursor()

            arg = (self._tenant_id, channel_id, sensor_id, val, ts)
            r = cursor.callproc("sp_insert_measurement_datapoint_with_timestamp", arg)
            cursor.close()
        except Exception as e:
            logger.error('sp_insert_measurement_datapoint_with_timestamp: failed - %s', e)
            return 0

        finally:
            cnx.close()

    # todo: legacy
    def sp_setChannelDiagnostics(self, channel_id, diagnostics_a, diagnostics_b, diagnostics_c, diagnostics_timestamp):
        logger.debug(f'sp_setChannelDiagnostics: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("INSERT INTO aoms.tbl_channeldiagnostics (tenant, channel, DIAGNOSTICS_A, "
                     "DIAGNOSTICS_B, DIAGNOSTICS_C, timestamp) "
                     "SELECT %s, c.id, %s, %s, %s, %s FROM aoms.tbl_channel AS c "
                     "WHERE c.channel_id = %s and c.isActive and c.isDiagnostic=%s and "
                     "c.tenant = %s;")
            cursor.execute(query, (self._tenant_id, diagnostics_a, diagnostics_b, diagnostics_c,
                                   diagnostics_timestamp, channel_id, self._diagnostic_mode,
                                   self._tenant_id))
            cnx.commit()
            cursor.close()
        finally:
            cnx.close()

    def sp_getSpectrumLargestID(self):

        logger.debug(f'sp_getSpectrumLargestID: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            # save to tbl_spectrum
            query = ("SELECT MAX(id) FROM aoms.tbl_spectrum where tenant = %s")
            cursor.execute(query, (self._tenant_id,))
            r = cursor.fetchone()
            cnx.commit()
            cursor.close()
            return r[0]
        finally:
            cnx.close()

    def sp_setSpectrum(self, channel_id, timestamp):

        logger.debug(f'sp_setSpectrum: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            # save to tbl_spectrum
            query = ("INSERT INTO aoms.tbl_spectrum (tenant, channel, timestamp) "
                     "SELECT %s, c.id, %s FROM aoms.tbl_channel AS c "
                     "WHERE c.channel_id = %s and c.isActive and c.isDiagnostic = %s and "
                     "c.tenant = %s;")
            cursor.execute(query, (self._tenant_id, timestamp, channel_id, self._diagnostic_mode,
                                   self._tenant_id))
            cnx.commit()
            cursor.close()
        finally:
            cnx.close()

    # todo: not tested
    def sp_setWavelength(self, spectrum_id, channel_id, sensor_ids, peaks, is_multi = 0):
        data_sets = []
        logger.debug(f'sp_setWavelength: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)

        try:
            # cnx.set_converter_class(NumpyMySQLConverter)        # to take care of peaks in numpy format

            cursor = cnx.cursor()
            qry_get_sensor_key = \
                "SELECT s.id " \
                "FROM tbl_sensor as s " \
                "JOIN tbl_channel as c ON c.id = s.channel " \
                "WHERE s.sensor_id = %s and c.channel_id = %s and s.isActive and c.isActive and " \
                "s.isDiagnostic = %s and c.isDiagnostic = %s and " \
                "s.tenant = %s and c.tenant = %s;"
            qry_insert_wavelength = "INSERT INTO tbl_wavelength (tenant, sensor, spectrum, peak) VALUES (%s, %s, %s, %s)"

            if is_multi:
                for s_id, p in zip(sensor_ids, peaks):
                    try:
                        cursor.execute(qry_get_sensor_key, (s_id, channel_id,
                                                            self._diagnostic_mode, self._diagnostic_mode,
                                                            self._tenant_id, self._tenant_id))
                        sensor_key = cursor.fetchone()[0]

                        data_set = (self._tenant_id, sensor_key, spectrum_id, p)
                        data_sets.append(data_set)

                    except Exception as e:
                        logger.error('Failed to fetch the sensor information - %s', e)
                        raise e
            else:
                try:
                    cursor.execute(qry_get_sensor_key, (channel_id, sensor_ids,
                                                        self._diagnostic_mode, self._diagnostic_mode,
                                                        self._tenant_id, self._tenant_id))
                    sensor_key = cursor.fetchone()[0]

                    data_sets = (self._tenant_id, sensor_key, spectrum_id, peaks)

                except Exception as e:
                    logger.error('Failed to fetch the sensor information - %s', e)
                    raise e
            try:
                cursor.executemany(qry_insert_wavelength, data_sets)

            except Exception as e:
                logger.error('Failed to insert wavelength - %s', e)
                raise e

            cursor.close()
        finally:
            # TODO: Follow the original logic, commit all completed updates.
            cnx.commit()
            cnx.close()

    def sp_getJobMetadata(self, msj_id):
        logger.debug(f'sp_getJobMetadata: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()

            query = (
                "SELECT t.name AS project_name, cjg.name AS job_group_name, cj.name AS job_name, cg.channel_group_id "
                "AS cable_id, n.node_id AS node_id, "
                "msj.concrete_profile, a.singular_threshold AS targeted_strength, cj.startDate, cj.terminationDate "
                "FROM tbl_maturity_strength_job msj "
                "INNER JOIN tbl_channel_job cj ON cj.id = msj.channel_job AND cj.tenant = msj.tenant "
                "LEFT JOIN tbl_alarm a ON a.id = cj.alarm "
                "INNER JOIN tbl_channel_job_group cjg ON cjg.id = cj.channel_job_group AND cjg.tenant = cj.tenant "
                "INNER JOIN tbl_channel_group cg ON cg.id = cj.channel_group AND cg.tenant = cj.tenant "
                "INNER JOIN tbl_node n ON n.channel_group = cg.id AND n.tenant = cg.tenant "
                "INNER JOIN tbl_tenant t ON t.id = msj.tenant "
                "WHERE msj.id = %s AND msj.tenant = %s;")
            cursor.execute(query, (msj_id, self._tenant_id))
            r = cursor.fetchone()
            cnx.commit()
            cursor.close()
            return r
        finally:
            cnx.close()

    # this does not include the blacklisted sensors
    def sp_getLatestAverageTemperatureStrength(self, msj_id):
        logger.debug(f'sp_getLatestAverageTemperatureStrength: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = ("SELECT ms1.tenant, ms1.maturity_strength_job, AVG(ms1.average_temperature) AS "
                     "latest_msj_average_temperature, AVG(ms1.strength) AS latest_msj_average_strength, MAX(ms1.timestamp) "
                     "AS latest_msj_timestamp FROM tbl_maturity_strength ms1 "
                     "INNER JOIN (SELECT ms3.tenant, ms3.sensor, MAX(ms3.timestamp) AS timestamp "
                     "FROM tbl_maturity_strength ms3 "
                     "WHERE ms3.maturity_strength_job = %s AND ms3.tenant = %s "
                     "GROUP BY ms3.sensor HAVING ms3.sensor NOT IN "
                     "(SELECT msbs.sensor FROM tbl_maturity_strength_blacklisted_sensor msbs "
                     "WHERE msbs.tenant = %s AND msbs.maturity_strength_job = %s)) "
                     "AS ms2 ON ms1.tenant = ms2.tenant AND ms1.sensor = ms2.sensor AND ms1.timestamp = ms2.timestamp;"
                     )
            cursor.execute(query, (msj_id, self._tenant_id, self._tenant_id, msj_id))
            r = cursor.fetchone()
            cnx.commit()
            cursor.close()
            return r[2:]
        finally:
            cnx.close()

    def sp_getHistoricalTemperatureMaturityStrength_DF(self, msj_id):
        logger.debug(f'sp_getHistoricalTemperatureMaturityStrength_DF: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            query = ("SELECT s.sensor_id, ms.average_temperature, ms.maturity_index, ms.strength, ms.timestamp "
                     "FROM tbl_maturity_strength ms INNER JOIN tbl_sensor s ON s.id = ms.sensor AND s.tenant = ms.tenant "
                     "WHERE ms.tenant = %s AND ms.maturity_strength_job = %s AND ms.sensor "
                     "ORDER BY ms.timestamp ASC, s.sensor_id ASC;"
                     )

            df = pd.read_sql(query % (self._tenant_id, msj_id),
                             con = cnx, parse_dates = ['timestamp'])
            return df
        finally:
            cnx.close()

    def sp_getBacklistedSensorID(self, msj_id):
        logger.debug(f'sp_getBacklistedSensorID: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = "SELECT s2.sensor_id FROM tbl_maturity_strength_blacklisted_sensor msbs " \
                    "INNER JOIN tbl_sensor s2 ON s2.tenant = msbs.tenant AND s2.id = msbs.sensor " \
                    "WHERE msbs.tenant = %s AND msbs.maturity_strength_job = %s;"
            cursor.execute(query, (self._tenant_id, msj_id))
            r = cursor.fetchall()
            cnx.commit()
            cursor.close()
            return r
        finally:
            cnx.close()

    def sp_getHistoricalTemperature_DF(self, msj_id):
        logger.debug(f'sp_getHistoricalTemperature_DF: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            query = "SELECT s.sensor_id, " \
                    "(CASE st.name " \
                    "WHEN 'Temperature' THEN lt.temperature " \
                    "WHEN 'Pressure' THEN lp.pressure " \
                    "ELSE lh.humidity END) AS measurement_data, " \
                    "(CASE st.name " \
                    "WHEN 'Temperature' THEN lt.timestamp " \
                    "WHEN 'Pressure' THEN lp.timestamp " \
                    "ELSE lh.timestamp END) AS timestamp " \
                    "FROM tbl_sensor s INNER JOIN tbl_channel c ON c.id = s.channel AND c.tenant = s.tenant " \
                    "INNER JOIN tbl_sensor_type st ON st.id = s.type " \
                    "INNER JOIN tbl_channel_group cg ON cg.id = c.channel_group AND cg.tenant = c.tenant " \
                    "INNER JOIN tbl_channel_job cj ON cj.channel_group = cg.id AND cj.tenant = cg.tenant " \
                    "INNER JOIN tbl_maturity_strength_job msj ON msj.channel_job = cj.id AND msj.tenant = cj.tenant " \
                    "LEFT JOIN ( SELECT t1.tenant, t1.temperature, t1.sensor, t1.timestamp FROM tbl_measurement_temperature_5min t1) lt " \
                    "ON lt.sensor = s.id AND lt.tenant = s.tenant AND st.name = 'Temperature' " \
                    "LEFT JOIN ( SELECT p1.tenant, p1.pressure, p1.sensor, p1.timestamp FROM tbl_measurement_pressure_5min p1) lp " \
                    "ON lp.sensor = s.id AND lp.tenant = s.tenant AND st.name = 'Pressure' " \
                    "LEFT JOIN ( SELECT h1.tenant, h1.humidity, h1.sensor, h1.timestamp FROM tbl_measurement_humidity_5min h1) lh " \
                    "ON lh.sensor = s.id AND lh.tenant = s.tenant AND st.name = 'Humidity' " \
                    "WHERE msj.id = %s " \
                    "AND msj.tenant = %s " \
                    "AND st.name = 'Temperature' " \
                    "AND s.sensor_id " \
                    "ORDER BY (CASE st.name " \
                    "WHEN 'Temperature' THEN lt.timestamp " \
                    "WHEN 'Pressure' THEN lp.timestamp " \
                    "ELSE lh.timestamp END) ASC, s.sensor_id ASC; "
            df = pd.read_sql(query % (msj_id, self._tenant_id),
                             con = cnx, parse_dates = ['timestamp'])
            return df
        finally:
            cnx.close()

    # todo: this query needs to be changed when new sensor type is available
    def sp_getHistoricalAggregatedTemperature_DF(self, msj_id):
        logger.debug(f'sp_getHistoricalAggregatedTemperature_DF: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            query = "SELECT (CASE st.name " \
                    "WHEN 'Temperature' THEN AVG(lt.temperature) " \
                    "WHEN 'Pressure' THEN AVG(lp.pressure) " \
                    "ELSE AVG(lh.humidity) END) AS average_data, " \
                    "(CASE st.name " \
                    "WHEN 'Temperature' THEN lt.timestamp " \
                    "WHEN 'Pressure' THEN lp.timestamp " \
                    "ELSE lh.timestamp END) AS timestamp " \
                    "FROM tbl_sensor s INNER JOIN tbl_channel c ON c.id = s.channel AND c.tenant = s.tenant " \
                    "INNER JOIN tbl_sensor_type st ON st.id = s.type " \
                    "INNER JOIN tbl_channel_group cg ON cg.id = c.channel_group AND cg.tenant = c.tenant " \
                    "INNER JOIN tbl_channel_job cj ON cj.channel_group = cg.id AND cj.tenant = cg.tenant " \
                    "INNER JOIN tbl_maturity_strength_job msj ON msj.channel_job = cj.id AND msj.tenant = cj.tenant " \
                    "LEFT JOIN ( SELECT t1.tenant, t1.temperature, t1.sensor, t1.timestamp FROM tbl_measurement_temperature_5min t1) lt " \
                    "ON lt.sensor = s.id AND lt.tenant = s.tenant AND st.name = 'Temperature' " \
                    "LEFT JOIN ( SELECT p1.tenant, p1.pressure, p1.sensor, p1.timestamp FROM tbl_measurement_pressure_5min p1) lp " \
                    "ON lp.sensor = s.id AND lp.tenant = s.tenant AND st.name = 'Pressure' " \
                    "LEFT JOIN ( SELECT h1.tenant, h1.humidity, h1.sensor, h1.timestamp FROM tbl_measurement_humidity_5min h1) lh " \
                    "ON lh.sensor = s.id AND lh.tenant = s.tenant AND st.name = 'Humidity' " \
                    "WHERE msj.id = %s " \
                    "AND msj.tenant = %s " \
                    "AND st.name = 'Temperature' " \
                    "AND s.sensor_id " \
                    "GROUP BY " \
                    "(CASE st.name " \
                    "WHEN 'Temperature' THEN lt.timestamp " \
                    "WHEN 'Pressure' THEN lp.timestamp " \
                    "ELSE lh.timestamp END);"

            df = pd.read_sql(query % (msj_id, self._tenant_id),
                             con = cnx, parse_dates = ['timestamp'])
            return df
        finally:
            cnx.close()

    def sp_getEventsByMaturityStrengthJob_DF(self, msj_id):
        logger.debug(f'sp_getEventsByMaturityStrengthJob_DF: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            df = pd.read_sql('CALL sp_get_events_by_msj(%s, %s);' % (self._tenant_id, msj_id), con = cnx)
            return df
        except Exception as e:
            logger.error('getEventsByMaturityStrengthJob failed', e)
            return 0
        finally:
            cnx.close()

    def sp_getAlarmsByMaturityStrengthJob_DF(self, msj_id):

        logger.debug(f'sp_getAlarmsByMaturityStrengthJob_DF: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            query = 'SELECT s.id, s.sensor_id, st.name AS sensorType, a.medium_threshold_lbound AS ' \
                    'lbound_threshold, a.medium_threshold_hbound AS hbound_threshold, a.creationDate ' \
                    'FROM tbl_sensor s INNER JOIN tbl_channel c ON c.id = s.channel AND c.tenant = s.tenant ' \
                    'INNER JOIN tbl_sensor_type st ON st.id = s.type ' \
                    'INNER JOIN tbl_channel_group cg ON cg.id = c.channel_group AND cg.tenant = c.tenant ' \
                    'INNER JOIN tbl_channel_job cj ON cj.channel_group = cg.id AND cj.tenant = cg.tenant ' \
                    'INNER JOIN tbl_maturity_strength_job msj ON msj.channel_job = cj.id AND msj.tenant = cj.tenant ' \
                    'INNER JOIN tbl_alarm a ON a.id = s.alarm ' \
                    'WHERE msj.tenant = %s AND msj.id = %s'

            df = pd.read_sql(query % (self._tenant_id, msj_id), con = cnx, parse_dates = ['timestamp'])
            return df
        finally:
            cnx.close()

    def sp_get_user_settings_timezone(self, user_name, company_id):

        logger.debug(f'sp_get_user_settings_timezone: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()

            query = "SELECT auv.val " \
                    "FROM tbl_user u " \
                    "INNER JOIN tbl_usersettings us ON us.user = u.id " \
                    "INNER JOIN tbl_usersetting_type ut ON ut.id = us.type " \
                    "INNER JOIN tbl_allowed_usersetting_values auv ON auv.id = us.allowed_usersetting_value " \
                    "WHERE u.username = %s AND u.company_id = %s AND ut.name IN ('Display Time Zone');"

            cursor.execute(query, (user_name, company_id))
            try:
                r = cursor.fetchone()[0]
            except Exception as e:
                r = 'UTC'
                logger.warning('sp_get_user_settings_timezone(%s, %s) failed - %s' % (user_name, company_id, e))

            cnx.commit()
            cursor.close()

            return r
        finally:
            cnx.close()

    def sp_get_user_settings_measurement_unit(self, user_name, company_id):
        logger.debug(f'sp_get_user_settings_measurement_unit: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()

            query = "SELECT auv.val " \
                    "FROM tbl_user u " \
                    "INNER JOIN tbl_usersettings us ON us.user = u.id " \
                    "INNER JOIN tbl_usersetting_type ut ON ut.id = us.type " \
                    "INNER JOIN tbl_allowed_usersetting_values auv ON auv.id = us.allowed_usersetting_value " \
                    "WHERE u.username = %s AND u.company_id = %s AND ut.name IN ('System of Measurement');"

            cursor.execute(query, (user_name, company_id))
            try:
                r = cursor.fetchone()[0]
            except Exception as e:
                r = 'Metric'
                logger.warning('sp_get_user_settings_measurement_unit(%s, %s) failed - %s' % (user_name, company_id, e))

            cnx.commit()
            cursor.close()

            return r
        finally:
            cnx.close()

    def sp_update_report_request_file_extension(self, rrId, file_ext):
        logger.debug(f'sp_update_report_request_file_extension: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            arg = (self._tenant_id, rrId, file_ext)
            r = cursor.callproc("sp_update_report_request_file_extension", arg)
            cnx.commit()
            cursor.close()
            return 1
        except Exception as e:
            logger.error('sp_update_report_request_file_extension failed - %s', e)
            return 0
        finally:
            cnx.close()

    def sp_update_report_request_status(self, rrId, status):
        logger.debug(f'sp_update_report_request_status: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()

            arg = (self._tenant_id, rrId, status)
            r = cursor.callproc("sp_update_report_request_status", arg)
            cnx.commit()
            cursor.close()
            return 1
        except Exception as e:
            logger.error('sp_update_report_request_status failed - %s', e)
            return 0

        finally:
            cnx.close()

    def sp_update_report_request_s3_file_info(self, rrId, s3_bucket_name, s3_file_key):
        logger.debug(f'sp_update_report_request_s3_file_info: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()

            arg = (self._tenant_id, rrId, s3_bucket_name, s3_file_key)
            r = cursor.callproc("sp_update_report_request_s3_file_info", arg)
            cnx.commit()
            cursor.close()
            return 1
        except Exception as e:
            logger.error('sp_update_report_request_s3_file_info failed - %s', e)
            return 0

        finally:
            cnx.close()

    def sp_update_report_request_start_date(self, rrId):
        logger.debug(f'sp_update_report_request_start_date: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            arg = (self._tenant_id, rrId)
            r = cursor.callproc("sp_update_report_request_start_date", arg)
            cnx.commit()
            cursor.close()
            return 1
        except Exception as e:
            logger.error('sp_update_report_request_start_date failed - %s', e)
            return 0
        finally:
            cnx.close()

    def sp_update_report_request_termination_date(self, rrId):
        logger.debug(f'sp_update_report_request_termination_date: {self._db_config}')
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()

            arg = (self._tenant_id, rrId)
            r = cursor.callproc("sp_update_report_request_termination_date", arg)
            cnx.commit()
            cursor.close()
            return 1
        except Exception as e:
            logger.error('sp_update_report_request_termination_date failed - %s', e)
            return 0
        finally:
            cnx.close()


# todo: legacy
class NumpyMySQLConverter(MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)
