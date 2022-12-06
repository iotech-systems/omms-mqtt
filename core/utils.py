
import time
import datetime


class utils(object):

   @staticmethod
   def ts_utc():
      t = datetime.datetime.utcnow()
      return f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}"

   @staticmethod
   def dts_utc():
      dt = datetime.datetime.utcnow()
      return f"{dt.year}:{dt.month:02d}:{dt.day:02d}" \
         f" {dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}"
