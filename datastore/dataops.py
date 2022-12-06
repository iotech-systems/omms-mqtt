
import redis
import datetime
import psycopg2

DB_DB = "sbms"
DB_PWD = "sbms_rest_api_pwd"
DB_USR = "sbms_rest_api"
DB_HOST = "10.0.0.122"
DB_PORT = 5432

RED_HOST = "10.0.0.122"
RED_PWD = "Q@@bcd!234##!"
RED_PORT = 6379
RED_DB = 0
RED_READS_CHANNEL = "OMMS_ELECTRIC_READS"


# noinspection PyTypeChecker
class dataOps(object):

   def __init__(self):
      self.conn: psycopg2.connection = None
      pool = redis.ConnectionPool(host=RED_HOST, port=RED_PORT, db=RED_DB, password=RED_PWD)
      self.redb = redis.Redis(connection_pool=pool)

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

   def elcrm_tag_dbid(self, m_dbid: int) -> str:
      qry: str = f"select elcrm_entag from config.meters t where t.meter_dbid = {m_dbid};"
      row = None
      with self.conn.cursor() as cur:
         cur.execute(qry)
         row = cur.fetchone()
         cur.close()
      # - - - - - - - - - - - - - - -
      if row is None:
         return None
      # -- got data --
      return str(row[0])

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
      done0: bool = False
      done1: bool = False
      with self.conn.cursor() as cur:
         cur.execute(ins)
         done0 = cur.rowcount == 1
         cur.connection.commit()
      # -- hb --
      try:
         with self.conn.cursor() as cur:
            cur.execute(f"delete from streams.__meter_heartbeats where fk_meter_dbid = {dbid};")
            self.conn.commit()
            cur.close()
         # -- get meter electric room tag --
         tag: str = self.elcrm_tag_dbid(dbid)
         payload = f"m_dbid: {dbid}; t_kWh: {t_kwh}; l1: {l1_t_kwh}; l2: {l2_t_kwh}; l3: {l3_t_kwh};"
         ins = f"insert into streams.__meter_heartbeats values({dbid}, '{tag}', now(), '{payload}');"
         with self.conn.cursor() as cur:
            cur.execute(ins)
            self.conn.commit()
            cur.close()
         # -- push to redis --
         self.publish_to_redis(tag, dbid, t_kwh, l1_t_kwh, l2_t_kwh, l3_t_kwh)
      except Exception as e:
         print(e)
      # -- return --
      return done0

   def publish_to_redis(self, tag: str, m_dbid: int, t_kwh, l1_kwh, l2_kwh, l3_kwh):
      try:
         msg = f"#MSG_TYPE:ELC_READ#[loc_tag: {tag}; m_dbid: {m_dbid}; t_kWh: {t_kwh}; " \
            f"l1_kwh: {l1_kwh}; l2_kwh: {l2_kwh}; l3: {l3_kwh}]#"
         self.redb.publish(RED_READS_CHANNEL, msg)
      except Exception as e:
         print(e)
