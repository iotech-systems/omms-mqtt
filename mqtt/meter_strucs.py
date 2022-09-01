

class regInfo(object):

   def __init__(self):
      self.dbid: str = ""
      self.regtype: str = ""
      self.data: float = 0.0
      self.dts: str = ""

   def to_str(self) -> str:
      return f"dbid: {self.dbid} | rtype: {self.regtype} |" \
         f" data: {self.data} | dts: {self.dts}"


# - - test - -
if __name__ == "__main__":
   mi: regInfo = regInfo()
   mi.data = "xxx"
   mi.regtype = "type"
   mi.dts = "dta"
   print(mi.to_str())
