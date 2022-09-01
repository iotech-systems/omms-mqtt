#!/usr/bin/env python3

import time
import typing as t
import xml.etree.ElementTree as et
from mqtt.mqtt_meter_reader import mqttMeterReader


def load_conf_xml() -> et.ElementTree:
   et_tree: et.ElementTree = et.parse("conf/meter_brokers.xml")
   return et_tree

def main_loop():
   lpcnt = 0
   while True:
      time.sleep(6.0)
      # -- -- -- -- -- --
      if lpcnt == 10:
         print("\t -- [ main_loop ] --")
         lpcnt = 0
      # -- -- -- -- -- --
      lpcnt += 1

def main():
   mqttMeters: t.List[mqttMeterReader] = []
   et_tree = load_conf_xml()
   # -- init meters --
   tree_root = et_tree.getroot()
   host = tree_root.attrib["host"]
   port: int = int(tree_root.attrib["port"])
   for meter in et_tree.findall("meters/meter"):
      tmp_meter = mqttMeterReader(meter)
      mqttMeters.append(tmp_meter)
      tmp_meter.init(host, port)
      tmp_meter.start()
   # -- start event listening --


# -- entry point --
if __name__ == "__main__":
   main()
   main_loop()

