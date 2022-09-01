
import time
import datetime


class utils(object):

   @staticmethod
   def ts_utc():
      t = datetime.datetime.utcnow()
      return f"{t.hour:02d}:{t.minute:02d}:{t.second:02d}"