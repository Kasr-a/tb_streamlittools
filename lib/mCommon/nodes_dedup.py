from datetime import datetime, timedelta
import time
import logging
logger = logging.getLogger(__name__)

SERVER_DEDUPLICATION_THRESHOLD = 0.2
gDedup = {}                     # a keyed list of nodes to keep track for deduplication purpose

class Nodelite(object):
    '''
    a lightweight class written for the purpose of keeping track of list of nodes for de-duplication purpose
    '''
    def __init__(self, eui):
        self.__eui = eui
        self.fcnt = 0
        self.timestamp = datetime.utcnow()
        self.lock = 0

    def update(self, fcnt, timestamp):
        self.fcnt = fcnt
        self.timestamp = timestamp


def is_duplicate_message(dev_eui, fcnt):
    global gDedup

    try:
        if gDedup[dev_eui].lock:
            logger.debug('%s sleeping 0.01' % dev_eui)
            time.sleep(0.01)

        gDedup[dev_eui].lock = 1

        if gDedup[dev_eui].fcnt == fcnt and \
                ((datetime.utcnow()-gDedup[dev_eui].timestamp) < timedelta(seconds=SERVER_DEDUPLICATION_THRESHOLD)):
            logger.debug('%s: found a duplicate - fcnt = %s (%s); timestamp = %s (%s)' %
                         (dev_eui, fcnt, gDedup[dev_eui].fcnt, datetime.utcnow(), gDedup[dev_eui].timestamp))
            gDedup[dev_eui].lock = 0
            return 1
        else:
            logger.debug('%s: not a duplicate - fcnt = %s (%s); timestamp = %s (%s), update node now' %
                         (dev_eui, fcnt, gDedup[dev_eui].fcnt, datetime.utcnow(), gDedup[dev_eui].timestamp))
    except KeyError:
        gDedup[dev_eui] = Nodelite(dev_eui)
        gDedup[dev_eui].lock = 1
        logger.debug('New node to track: %s' % dev_eui)
    finally:
        logger.debug('update node list')
        gDedup[dev_eui].update(fcnt, datetime.utcnow())
        gDedup[dev_eui].lock = 0

    return 0
