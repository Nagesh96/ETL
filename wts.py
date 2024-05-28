def delete files(self):

Deletes files after completing the process

Args:

none

Returns:

none

Raises:

try:

Exception on pulling files or deleting files

files pathos.path.join(os.getcwd(), '*WTS Trade_Data".txt')

#logging.info('WTS 100 came to delete files method files path: '+str(files_path))

files sorted(

glob.iglob(files_path), key os.path.getctime, reverse-True)

logging.info('WTS 100 came to delete files method files str(files))

except Exception as e:

try:

logging.info('Exception error is + str(e))

logging.info("Exception occurred on pulling WTS 100 files for deletion')

for f in files:

os.remove(f)

#logging.info('WTS 100 came to delete files files try ended: ')

except Exception as e:

logging.info("Exception error is: '+ str(e))

logging.info("Exception occurred on deleting files')

logging.info('Successfully deleted files')

return

def main(self):

self.delete_files()

wts_data self.get_data()

self.insert data(wts_data) if self.error status = False:

self.batch_log success()

self.delete_files()
return
