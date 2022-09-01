
import os, time
import threading
import typing as t
import xml.etree.ElementTree as et
import paho.mqtt.client as mqtt
from core.utils import utils
from core import core
from mqtt.meter_strucs import regInfo
from datastore.dataops import dataOps


# noinspection PyTypeChecker
class mqttMeterReaderV1(object):

   def __init__(self, xmlconf: str):
      self.xmlconf = xmlconf
      self.xmldoc: et.ElementTree = None
      self.meters: t.List[et.Element] = None
      self.regs: t.List[et.Element] = None
      self.rt_thread = threading.Thread(target=self.__rt_thread__, args=(None,))
      self.clt: mqtt.Client = mqtt.Client()
      self.meter_table: {} = {}
      self.host: str = ""
      self.port: int = 0
      self.pwd: str = ""
      self.meter_reads: {} = {}
      self.on_msg_lock: threading.Lock = threading.Lock()
      self.last_report: int = 0
      self.report_interval = 120

   def on_connect(self, clt, ud, flags, rc):
      print("\t[ on_connect ]")
      this: mqttMeterReaderV1 = ud
      topics = this.meter_reads.keys()
      for topic in topics:
         this.clt.subscribe(topic=topic)

   def on_msg(self, _, ud, msg):
      this: mqttMeterReaderV1 = ud
      try:
         # - - - - - - - -
         this.on_msg_lock.acquire()
         # - - - - - - - -
         msg: mqtt.MQTTMessage = msg
         minfo: regInfo = this.meter_reads[msg.topic]
         minfo.data = round(float(msg.payload), 3)
         minfo.dts = utils.ts_utc()
         this.meter_reads[msg.topic] = minfo
         # - - - - - - - -
      except Exception as e:
         print(e)
      finally:
         this.on_msg_lock.release()

   def mqtt_init(self):
      # -- callbacks --
      self.clt.on_connect = self.on_connect
      self.clt.on_message = self.on_msg
      # -- set user data --
      self.clt.user_data_set(self)
      self.clt.connect(self.host, self.port)

   def load_conf(self) -> bool:
      try:
         # look in curdir/conf folder
         xmlfile = f"conf/{self.xmlconf}"
         if not os.path.exists(xmlfile):
            print(f"FileNotFound: {xmlfile}")
            exit(1)
         # -- hp --
         self.xmldoc: et.ElementTree = et.parse(xmlfile)
         self.__init_xml_conf__()
         self.__init_meter_tbl__()
         # -- return --
         return True
      except Exception as e:
         print(e)
         return False

   def start(self) -> bool:
      self.rt_thread.start()
      self.clt.loop_start()
      return True

   def __init_meter_tbl__(self):
      for meter in self.meters:
         self.__create_meter__(meter)

   def __create_meter__(self, m: et.Element):
      # - - - - - - - - - - - - - - - - - - - -
      def get_type(typ: str) -> [str, None]:
         for r in self.regs:
            if r.attrib["type"] == typ:
               return r
         # -- end --
         return None
      # - - - - - - - - - - - - - - - - - - - -
      # tag = m.attrib["tag"]
      mid = m.attrib["id"]
      dbid = m.attrib["dbid"]
      regs: t.List[et.Element] = m.findall("reg")
      # - - - - - - - - - - - - - - - - - - - -
      for reg in regs:
         minfo: regInfo = regInfo()
         minfo.data = ""
         minfo.dts = utils.ts_utc()
         minfo.regtype = reg.attrib["type"]
         minfo.dbid = dbid
         # - - - - - - - - - - - - - - -
         treg = get_type(minfo.regtype)
         tpath = treg.attrib["path"]
         tmpl = reg.attrib["tmpl"]
         topic = tmpl.format(mid=mid, tpath=tpath)
         self.meter_reads[topic] = minfo

   def __init_xml_conf__(self):
      root = self.xmldoc.getroot()
      self.host = root.attrib["host"]
      self.port = int(root.attrib["port"])
      self.pwd = root.attrib["pwd"]
      self.meters: t.List[et.Element] = root.findall("meters/meter")
      self.regs: t.List[et.Element] = root.findall("regtable/reg")

   def __report__(self):
      # - - - - - - - - - - - - - - - -
      dbops: dataOps = dataOps()
      try:
         # - - - - - - - - - - - - - - - -
         if not dbops.connect():
            return
         # -- hp --
         self.on_msg_lock.acquire()
         for m in self.meters:
            mid = m.attrib["id"]
            print(f"\n{mid}")
            # - - - - - - - - - - - -
            last_dbid: int = 0
            meter_data: {} = {}
            for key in self.meter_reads.keys():
               if str(key).startswith(mid):
                  rinfo: regInfo = self.meter_reads[key]
                  meter_data[rinfo.regtype] = rinfo
                  last_dbid = rinfo.dbid
            # -- hp --
            if dbops.save_kwhrs(last_dbid, meter_data):
               print(f"DataSaved @dbid: {last_dbid}")
         # - - - - - - - - - - - - - - - -
      except Exception as e:
         print(e)
      finally:
         self.on_msg_lock.release()
         dbops.close()

   def __rt_thread__(self, args):
      self.mqtt_init()
      # -- hp --
      while True:
         time.sleep(core.LOOP_SLEEP_SECS)
         epoch_time = int(time.time())
         if (epoch_time - self.last_report) > core.KWH_REPORT_INTERVAL:
            self.__report__()
            self.last_report = int(time.time())
