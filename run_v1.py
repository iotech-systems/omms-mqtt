#!/usr/bin/env python3

import time
import setproctitle
from core import core
from mqtt.meter_reader import mqttMeterReaderV1


XML_CONF_FILE = "mqtt_meters.xml"
mqttReaderV1: mqttMeterReaderV1 = mqttMeterReaderV1(XML_CONF_FILE)


def main():
   if not mqttReaderV1.load_conf():
      pass
   # -- hp --
   mqttReaderV1.mqtt_init()
   # -- hp --
   if not mqttReaderV1.start():
      exit(1)
   # -- hp --
   while True:
      time.sleep(core.LOOP_SLEEP_SECS)


# -- start here --
if __name__ == "__main__":
   setproctitle.setproctitle("omms-mqtt")
   main()
