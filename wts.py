try:

connection engine.raw_connection()

cursor = connection.cursor()

cursor.execute(exec_string, (batch_id, batch_id, error_summary, batch_id))

cursor.commit()

connection.close()

except Exception as e:

logging.exception('Exception on batch log error update')

return

def formatWTSFile(self, wts):

try:

wts [['Match Sent', 'Match Received', 'Amender', 'Authorizer', 'ReasonCode',

'SpecificReason', 'Market', 'Investment Strategy',

'Desk Location', 'Sell CCY', 'Buy CCY', 'STP', 'Non-STP',

Late Trade', 'On Time Trade', 'Matched on T'

'Matched on T +1', 'Matched on T+2_Greater', 'Market Region', 'Account Region Owner', 'Trade Region Owner', 'FX Region Owner',

'Holding Rule ID', 'Holding Rule Description', 'OSP External Value', 'PSET',

CLS', 'Settlement Currency', 'Trade Currency', 'Trade Touched by 100 FX', ' 'Block Ref', 'Settlement Amount', 'SEDOL', 'ISIN', 'Broker Interface Operator',

'Trade Reinstruct Operator', 'SSIs Reattach Operator',

'Entered Status Operator', 'Asset Related Status Operator', 'FX Agent/CompleteFX Destination',

'Block Mismatch', 'Allocation Mismatch', 'Block Match', 'Allocation Match', 'Match Status', 'Block Force Match', 'Allocation Force Match', 'PSAFE Validation', 'Failed Block Validation',

'Failed Allocation Validation', 'Type', 'Affirmation']] = wts [['Match Sent', 'Match Received', 'Amender',
