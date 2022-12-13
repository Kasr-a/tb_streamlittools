import base64
import logging
import re

import mysql.connector

logger = logging.getLogger(__name__)


class Alvin(object):

    def __init__(self, db_config):
        self._db_config = db_config

    def ping_alvin(self):
        cnx = None
        try:
            cnx = mysql.connector.connect(**self._db_config)
            logger.info("Established Alvin MySQL connection")
            return 1
        except:
            logger.warning("Alvin MySQL connection failed!")
            return 0
        finally:
            if cnx:
                cnx.close()

    def sp_getInternalSetting(self, setting_key):
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = "SELECT gs.val FROM alvin.tbl_internalsettings AS gs WHERE gs.key = %s"
            cursor.execute(query, (setting_key,))
            result = cursor.fetchone()
            cursor.close()
            return result[0]
        except Exception as e:
            logger.error('sp_getInternalSetting error - %s', e)
            return 0
        finally:
            cnx.close()

    def sp_setInternalSetting(self, setting_key, setting_val):
        cnx = mysql.connector.connect(**self._db_config)
        try:

            cursor = cnx.cursor()
            query = "UPDATE tbl_internalsettings AS gs SET gs.val = %s WHERE gs.key = %s;"
            cursor.execute(query, (setting_val, setting_key))
            cnx.commit()
            cursor.close()
            return 1
        except Exception as e:
            logger.warning('sp_setInternalSetting error - %s' % e)
            return 0
        finally:
            cnx.close()

    def sp_get_gateway_map(self):
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor(dictionary = True)
            cursor.callproc("sp_get_gateways")
            result = []
            for recordset in cursor.stored_results():
                for row in recordset:
                    result.append(dict(zip(recordset.column_names, row)))
            cursor.close()
            return result
        except Exception as e:
            logger.error('Get gateway map failed - %s', e)
            return 0

        finally:
            cnx.close()

    def sp_updateGatewayLastSeen(self, gateway_id):
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (gateway_id,)
            r = cursor.callproc("sp_update_gateway_lastSeen", args)
            cnx.commit()
            cursor.close()
            return 1
        except Exception as e:
            logger.error('Update Gateway Last Seen failed %s - %s' % (gateway_id, e))
            return 0
        finally:
            cnx.close()

    def sp_updateNodeLastSeen(self, node_id):
        """
        Last seen means the message from the node is a valid and decoded
        """
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (node_id,)
            r = cursor.callproc("sp_update_node_lastSeen", args)
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.error('Update Node Last Seen failed %s - %s' % (node_id, e))
            return 0
        finally:
            cnx.close()

    def sp_updateNodeLastSignal(self, node_id):
        """
        Last signal means getting any activity from the node
        """
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (node_id,)
            r = cursor.callproc("sp_update_node_lastSignal", args)
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.error('Update Node Last Seen failed %s - %s' % (node_id, e))
            return 0
        finally:
            cnx.close()

    # input: tenant_id
    # output: keyed dictionary of following keys
    #           company_name
    #           project_name
    #           company_id
    def sp_get_tenant_info(self, tenant_id):
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            query = "SELECT c.name AS companyName, t.name AS projectName, c.company_id AS companyId " \
                    "FROM tbl_tenant t " \
                    "INNER JOIN tbl_company c ON c.id = t.company " \
                    "WHERE t.id = %s;"
            cursor.execute(query, (tenant_id,))
            r = cursor.fetchone()
            cnx.commit()
            cursor.close()
        except Exception as e:
            logger.error('sp_get_tenant_info error - %s', e)
            return 0
        finally:
            cnx.close()

        dict = {}
        try:
            dict['company_name'] = r[0]
            dict['project_name'] = r[1]
            dict['company_id'] = r[2]
        except Exception as e:
            logger.warning('unable to retrieve tenant info, use default')
            dict['company_name'] = 'Unknown Company'
            dict['project_name'] = 'Unknown Project'
            dict['company_id'] = 'Unknown Company ID'

        return dict

    # given tenant id (t_id), get the associated logo file from database
    # if more than 1 logo file exists, return the first one found
    def get_company_logo_by_tenant_id(self, t_id):

        tenant_info = self.sp_get_tenant_info(t_id)

        if not tenant_info and not tenant_info['company_id']:
            return None, None

        assets = self.sp_get_assets_by_company_id(tenant_info['company_id'])

        if not assets:
            return None, None

        for a in assets:
            if a['type'] == 'LOGO_IMG':

                try:
                    logger.debug("attempt to decode blob for file: %s" % a['name'])
                    # get only the binary value of the blob
                    str_bin_val = a['bin_val'].split(',')  # .decode('ascii')
                    ext_raw = re.split('[/;,]', str_bin_val[0])[1]

                    logger.debug('blob format = %s' % ext_raw)

                    if ext_raw == 'svg+xml' or ext_raw == 'svg':
                        ext = 'svg'
                    elif ext_raw == 'png':
                        ext = 'png'
                    else:
                        logger.warning('Only SVG and PNG logo files are supported')
                        return None, None

                    # encode it to byte array
                    byte_array = str.encode(str_bin_val[1])
                    data = base64.decodebytes(byte_array)

                    return data, ext

                except Exception as e:
                    logger.warning("Decode blob images failed - company id: %s | %s" % (t_id, e))
                    return None, None

        return None, None

    def sp_get_assets_by_company_id(self, c_id):
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor(dictionary = True)
            args = (c_id,)
            cursor.callproc("sp_get_company_assets_by_companyId", args)
            result = []
            for recordset in cursor.stored_results():
                for row in recordset:
                    result.append(dict(zip(recordset.column_names, row)))
            cursor.close()
            return result
        except Exception as e:
            logger.error('sp_get_company_assets_by_companyId failed - %s', e)
            return 0
        finally:
            cnx.close()

    def sp_get_node_map(self):
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor(dictionary = True)
            cursor.callproc("sp_get_node_info")
            result = []
            for recordset in cursor.stored_results():
                for row in recordset:
                    result.append(row)
            cursor.close()
            return result

        except Exception as e:
            logger.error(f'Get node info failed - {get_error_trace(e)}')
            return 0
        finally:
            cnx.close()

    # todo: legacy
    def sp_get_node_info_by_node_id(self, node_id):
        cnx = mysql.connector.connect(**self._db_config)
        try:
            cursor = cnx.cursor()
            args = (node_id,)
            r = cursor.callproc("sp_get_node_info_by_node_id", args)
            cnx.commit()
            cursor.close()
            return r
        except Exception as e:
            logger.error('Get node info failed - %s', e)
            return 0
        finally:
            cnx.close()


import traceback

def get_error_trace(err: Exception, request_data: any = None) -> dict:
    trace = {"Detail": f'{err}:{"".join(traceback.format_tb(err.__traceback__))}'}
    if request_data:
        trace["Data"] = request_data
    return trace