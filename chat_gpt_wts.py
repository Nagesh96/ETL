import pandas as pd
import time
import logging
import re
import datetime
from datetime import timedelta
from sqlalchemy import create_engine
import glob
import os
import ftplib
import numpy as np

logging.basicConfig(level=logging.INFO)
logging.info('Starting WTS job')

class WTSInsert:
    def __init__(self, batch_id, params):
        self.batch_id = batch_id
        self.params = params
        logging.getLogger().setLevel(logging.INFO)
        self.error_status = False

    def batch_log_success(self):
        engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)
        batch_id = str(self.batch_id)
        exec_string = """ 
        DECLARE @curDate datetime2(7)
        SET @curDate = CURRENT_TIMESTAMP
        UPDATE [daedbo].[dae_fabi_batch_log] 
        SET s_batch_end_date = @curDate, t_batch_status = 'SUCCESS' 
        WHERE i_batch_id = ?
        """
        try:
            with engine.connect() as connection:
                connection.execute(exec_string, batch_id)
        except Exception as e:
            logging.exception('Exception on batch log success update')

    def batch_log_error(self, error_summary):
        engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)
        batch_id = str(self.batch_id)
        self.error_status = True
        exec_string = """
        DECLARE @curDate datetime2(7)
        SET @curDate = CURRENT_TIMESTAMP
        UPDATE [daedbo].[dae_fabi_batch_log] 
        SET s_batch_end_date = @curDate, t_batch_status = 'ERROR', t_batch_error_msg = ?
        WHERE i_batch_id = ?
        """
        try:
            with engine.connect() as connection:
                connection.execute(exec_string, error_summary, batch_id)
        except Exception as e:
            logging.exception('Exception on batch log error update')

    def formatWTSFile(self, wts):
        try:
            columns = ['Match Sent', 'Match Received', 'Amender', 'Authorizer', 'ReasonCode',
                       'SpecificReason', 'Market', 'Investment Strategy', 'Desk Location',
                       'Sell CCY', 'Buy CCY', 'STP', 'Non-STP', 'Late Trade', 'On Time Trade',
                       'Matched on T', 'Matched on T+1', 'Matched on T+2_Greater', 'Market Region',
                       'Account Region Owner', 'Trade Region Owner', 'FX Region Owner',
                       'Holding Rule ID', 'Holding Rule Description', 'OSP External Value', 'PSET',
                       'CLS', 'Settlement Currency', 'Trade Currency', 'Trade Touched by 100 FX',
                       'Block Ref', 'Settlement Amount', 'SEDOL', 'ISIN', 'Broker', 'Interface Operator',
                       'Trade Reinstruct Operator', 'SSIs Reattach Operator', 'Entered Status Operator',
                       'Asset Related Status Operator', 'FX Agent/CompleteFX Destination', 'Block Mismatch',
                       'Allocation Mismatch', 'Block Match', 'Allocation Match', 'Match Status',
                       'Block Force Match', 'Allocation Force Match', 'PSAFE Validation', 
                       'Failed Block Validation', 'Failed Allocation Validation', 'Type', 'Affirmation']

            missing_cols = set(columns) - set(wts.columns)
            for col in missing_cols:
                wts[col] = ''

            wts = wts[columns].replace(np.nan, '', regex=True)
            wts = wts.astype(str)
            wts['Match Received Date'] = pd.to_datetime(wts['Match Received'])
            wts = wts.sort_values(by=['Trade ID', 'Match Received Date'])
            wts.drop_duplicates(subset='Trade ID', keep='first', inplace=True)
            del wts['Match Received Date']
            wts = wts.loc[wts['Trade ID'] != 'nan']
            wts['Trade Touched by 100 FX'] = wts['Trade Touched by 100 FX'].apply(
                lambda x: 'Y' if x == 'Yes' else ('N' if x == 'No' else x))
            wts['Company'] = wts['Company'].replace(r'\n', '', regex=True)
            wts['Settlement Amount'] = wts['Settlement Amount'].replace(r',', '', regex=True)
            return wts
        except Exception as e:
            logging.exception("Exception in formatWTSFile")
            raise

    def get_data(self):
        filelist = []
        try:
            ftp_host = os.environ.get('FTP_SERVER')
            ftp_pass = os.environ.get('FTP_PASSWORD')
            ftp_user = os.environ.get('FTP_USERNAME')
            f = ftplib.FTP(ftp_host)
            f.login(user=ftp_user, passwd=ftp_pass)
            f.cwd('/ftp-fund/Capacity Model/')
            logging.info('FTP Login Success')
        except ftplib.error_perm:
            logging.exception('FTP Login Failed')
            return

        data = []
        f.dir(data.append)
        current_year_month = datetime.datetime.today().strftime("%Y-%m")
        logging.info('Current Year-Month: ' + current_year_month)

        # Filter files based on the current year, month, and specified patterns
        filtered_files = [line for line in data if current_year_month in line and ('Non-NTAM' in line or 'NTAM' in line)]

        if not filtered_files:
            logging.info('No matching files found')
            f.quit()
            return

        datelist = []
        filelist = []
        
        for line in filtered_files:
            parts = line.split()
            filename = parts[-1]
            datestr = ' '.join(parts[0:2])
            date = time.strptime(datestr, '%m-%d-%y %H:%M%p')
            datelist.append(date)
            filelist.append(filename)

        combo = list(zip(datelist, filelist))
        combo.sort(key=lambda x: x[0], reverse=True)

        latest_files = [file for _, file in combo[:2]]

        try:
            for filename in latest_files:
                with open(filename, 'wb') as f_local:
                    f.retrbinary(f'RETR {filename}', f_local.write)
                logging.info(f'Downloaded: {filename}')

            f.quit()
            files_path = os.path.join(os.getcwd(), '*WTS_Trade_Data*.txt')
            logging.info('WTS 100 get data method files path is_step_2: ' + str(files_path))
            files = sorted(glob.iglob(files_path), key=os.path.getctime, reverse=True)
            logging.info('WTS 100-get data method files is step_3: ' + str(files))
        except Exception as e:
            logging.info('WTS IOO exception is: '+str(e)) 
            logging.exception('Exception on downloading Security Detail files')
            self.batch_log_error(str(e))

        if self.error_status == False:
            try:
                foundFileList = []
                for file in files:
                    if file.find(datetime.datetime.today().strftime('%Y-%m'), 0, len(file)) > 0:
                        # It's the current month
                        foundFileList.append(file)

                for file in foundFileList[0:2]:
                    wts = pd.read_csv(file, sep='\t', lineterminator='\r', encoding="ISO-8859-1", error_bad_lines=False)
                    if 'PSET' not in wts.columns.tolist():
                        wts['PSET'] = ''
                        wts['CLS'] = ''
                        wts['Settlement Currency'] = ''
                        wts['Trade Currency'] = ''
                    if 'Completed/Cancelled' in wts.columns.tolist():
                        wts['Completed/Cancelled Date'] = wts['Completed/Cancelled']
                        del wts['Completed/Cancelled']
                    try:
                        wts = self.formatWTSFile(wts)
                        print('cleaned up file')
                    except Exception as e:
                        logging.info('WTS IOO exception is: ' + str(e))
            except Exception as e:
                self.batch_log_error(str(e))
                logging.exception('Exception on cleaning WCM file')
        else:
            return

    def insert_data(self, wts):
        try:
            logging.info('Started WTS 100 insert process')
            engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)
            connection = engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute("IF OBJECT_ID('tempdb..##wts') IS NOT NULL DROP TABLE ##wts")
            cursor.commit()
            table_name = '##wts'
            wts.to_sql(table_name, engine, if_exists='replace', chunksize=True)
            connection = engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute("{CALL [daeproc].[dae_loobi_insert_wts_trades]()}")
            cursor.commit()
            connection.close()
        except Exception as e:
            self.batch_log_error(str(e))
            logging.exception('Exception occurred on WTS 100 insert process')

    def delete_files(self):
        """
        Deletes files after completing the process
        Args:
            none
        Returns:
            none
        Raises:
            Exception on pulling files or deleting files
        """
        try:
            files_path = os.path.join(os.getcwd(), '*WTS_Trade_Data*.txt')
            logging.info('WTS 100 came to delete files method files path: ' + str(files_path))
            files = sorted(glob.iglob(files_path), key=os.path.getctime, reverse=True)
            logging.info('WTS 100 came to delete files method files: ' + str(files))
        except Exception as e:
            logging.info('Exception error is: ' + str(e))
            logging.info('Exception occurred on pulling WTS 100 files for deletion')
        try:
            for f in files:
                os.remove(f)
            logging.info('Successfully deleted files')
        except Exception as e:
            logging.info('Exception error is: ' + str(e))
            logging.info('Exception occurred on deleting files')

    def main(self):
        self.delete_files()
        wts_data = self.get_data()
        self.insert_data(wts_data)
        if self.error_status == False:
            self.batch_log_success()
        self.delete_files()
        return
        
