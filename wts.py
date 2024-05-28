try:

connection engine.raw_connection()

cursor = connection.cursor()

cursor.execute(exec_string)

cursor.commit()

connection.close()

except Exception as e:

logging.exception('Exception on batch log success update')

I

return

def batch_log_error(self, error_summary):

engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)

batch_id= str(self.batch_id) True

self.error_status

exec_string = """

DECLARE @curDate datetime2(7)

SET @curDate CURRENT_TIMESTAMP

UPDATE [daedbo].[dae_fabi_batch_log] set s_batch_end_date @curDate where i_batch_id = ? UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_status = 'ERROR' where i_batch_id = ?

UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_error_msg? where i_batch_id = ?
