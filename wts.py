import pandas as pd

import time

import logging

import re

import datetime

from datetime import timedelta

from sqlalchemy import create_engine

import glob, os

import ftplib

logging.info('Starting WTS job')

class WTSInsert:

def __init__(self, batch_id, params):

self.batch_id = batch_id

self.params = params

logging.getLogger().setLevel(logging.INFO)

self.error_status = False

def batch_log_success(self):

engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)

batch_id= str(self.batch_id)

exec_string =

DECLARE @curDate datetime2(7)

SET @curDate CURRENT_TIMESTAMP

+ batch_id +

UPDATE [daedbo].[dae_fabi_batch_log] set s_batch_end_date = @curDate where i_batch_id = UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_status = 'SUCCESS' where i_batch_id =

+ batch_id
