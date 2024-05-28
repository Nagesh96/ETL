files pathos.path.join(os.getcwd(), "WTS_Trade_Data*.txt')

logging.info('WTS 100 get data method files path is_step_2 + str(files_path)) files sorted(glob.iglob(files_path), key-os.path.getctime, reverse=True)

logging.info('WTS 100-get data method files is step_3:'+ str(files))

except Exception as e:

logging.info("Security Detail exception is: '+str(e)) logging.exception('Exception on downloading Security Detail files')

self.batch_log_error(str(e))

if self.error status False:

try:

foundFileList = []

for file in files:

if file.find(datetime.datetime.today().strftime('%Y-%m'),0,len(file)) > 0:

#its the current month

foundFileList.append(file)

for file in foundFileList[0:2]:

wts pd.read_csv(file, sep-'\t', lineterminator-'\r', encoding "ISO-8859-1", error bad_lines=False) #wts pd.read_csv(file, sep='\t', lineterminator='\r', encoding "ISO-8859-1", on_bad_lines-'skip')

if 'PSET' not in wts.columns.tolist():

wts['PSET'] =

wts['CLS'] =

wts['Settlement Currency] =

wts['Trade Currency'] =

if 'Completed/Cancelled in wts.columns.tolist(): wts['Completed/Cancelled Date'] wts['Completed/Cancelled"]

try:

del wts['Completed/Cancelled']

wts formatWTSFile(self, wts)

print('cleaned up file)

except Exception as e:

writeException(str(e), 'Error on formatting WTS file')
