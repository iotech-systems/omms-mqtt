
import datetime
import psycopg2

DB_DB = "sbms"
DB_PWD = "sbms_rest_api_pwd"
DB_USR = "sbms_rest_api"
DB_HOST = "10.0.0.122"
DB_PORT = 5432


# noinspection PyTypeChecker
class dataOps(object):

   def __init__(self):
      self.conn: psycopg2.connection = None

   def connect(self) -> bool:
      self.conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_DB, user=DB_USR, password=DB_PWD)
      if self.conn is None or self.conn.closed != 0:
         print("UnableToConnect")
         return False
      # -- hp --
      print("ConnectedToDatabase")
      return True

   def close(self):
      if self.conn.closed != 0:
         self.conn.close()
      if self.conn.closed == 0:
         print("DatabaseConnClosed")

   def dbid_exists(self, dbid: int) -> bool:
      try:
         # - - - - - - - - - - - - - - -
         sel = f"select count(*) cnt from config.meters m where m.meter_dbid = {dbid};"
         with self.conn.cursor() as cur:
            cur.execute(sel)
            """ Fetch the next row of a query result set, returning a single tuple, 
               or None when no more data is available: """
            row = cur.fetchone()
         # - - - - - - - - - - - - - - -
         if row is None:
            return None
         # -- got data --
         return int(row[0]) == 1
         # - - - - - - - - - - - - - - -
      except Exception as e:
         print(e)

   def save_kwhrs(self, dbid: int,  md: {}) -> bool:
      # - - - - - - - - - - - - - - -
      if not self.dbid_exists(dbid):
         return False
      # -- hp --
      t_kwh = md["total_kwh"].data
      l1_t_kwh = md["l1_total_kwh"].data
      l2_t_kwh = md["l2_total_kwh"].data
      l3_t_kwh = md["l3_total_kwh"].data
      dtsutc = datetime.datetime.utcnow()
      ins = f"insert into streams.kwhrs values({dbid}, cast('{dtsutc}' as timestamp)," \
         f" 0.28, {t_kwh}, {l1_t_kwh}, {l2_t_kwh}, {l3_t_kwh}, default);"
      print(ins)
      done: bool = False
      with self.conn.cursor() as cur:
         cur.execute(ins)
         done = cur.rowcount == 1
         cur.connection.commit()
      # -- hp --
      return done
