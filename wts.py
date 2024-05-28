except Exception as e:

self.batch_log_error(str(e))

logging.exception('Exception on cleaning WCM file')

else:

return

def insert data(self, wts):

try:

logging.info("Started WTS 100 insert process')

engine create_engine('mssql+pyodbc:///?odbc_connect-%s' % self.params, fast_executemany=True)

connection = engine.raw_connection()

cursor connection.cursor()

cursor.execute("IF OBJECT_ID('tempdb..##wts') IS NOT NULL DROP TABLE ##wts")

cursor.commit()

table_name = '##wts"

wts.to sql(table name, engine, if_exists 'replace', chunksize = True)

connection engine.raw_connection()

cursor connection.cursor()

cursor.execute("{CALL [daeproc].[dae_loobi_insert_wts_trades].]")

cursor.commit()

connection.close()

except Exception as e:

self.batch_log_error(str(e))

logging.exception("Exception occurred on WTS 100 insert process')
