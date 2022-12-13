# This is the file used to generate all of the default settings for Mia

import ConfigParser
import os

config = ConfigParser.RawConfigParser()

config.add_section('ODAQ')
config.add_section('HYPERION')
config.add_section('LOGGER')

config.set('ODAQ', 'SIMULATION_MODE', 1)
config.set('ODAQ', 'MAX_CHANNEL_N', 36)
config.set('ODAQ', 'simulation_w_start', 1534.5)
config.set('ODAQ', 'simulation_w_len', 10)
config.set('ODAQ', 'simulation_dw', 3)


config.set('HYPERION', 'IP_ADDRESS','10.0.0.55')
config.set('HYPERION', 'DEFAULT_CHANNEL',1)             # Channel index for Hyperion instrument,1.
config.set('HYPERION', 'TIMEOUT',1000)             # Unit: milliseconds
config.set('LOGGER', 'PATH_LINUX','/var/log/aoms/')
config.set('LOGGER', 'PATH_WINDOWS','C:\AOMS\Log')


# Writing our configuration file to 'example.cfg'
with open(os.path.join('..','settings-n.cfg'), 'wb') as configfile:
    config.write(configfile)