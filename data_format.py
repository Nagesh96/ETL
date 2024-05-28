def formatWTSFile(self, wts):
    try:
        # Defining the columns to be formatted
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

        # Replacing NaN values with an empty string
        wts[columns] = wts[columns].replace(np.nan, '', regex=True)
        
        # Converting all columns to string type
        wts = wts.astype(str)
        
        # Parsing and sorting by 'Match Received' date
        wts['Match Received Date'] = pd.to_datetime(wts['Match Received']) 
        wts = wts.sort_values(by=['Trade ID', 'Match Received Date'])
        
        # Dropping duplicate 'Trade ID' entries and removing the temporary 'Match Received Date' column
        wts = wts.drop_duplicates(subset='Trade ID', keep='first')
        del wts['Match Received Date'] 
        
        # Filtering out rows with 'Trade ID' as NaN
        wts = wts.loc[wts['Trade ID'] != 'nan']
        
        # Formatting the 'Trade Touched by 100 FX' column
        wts['Trade Touched by 100 FX Mod'] = wts['Trade Touched by 100 FX'].apply(lambda x: 'Y' if x == 'Yes' else ('N' if x == 'No' else x))
        wts['Trade Touched by 100 FX'] = wts['Trade Touched by 100 FX Mod']
        del wts['Trade Touched by 100 FX Mod']
        
        # Cleaning up 'Company' and 'Settlement Amount' columns
        wts['Company'] = wts['Company'].replace(r'\n', '', regex=True)
        wts['Settlement Amount'] = wts['Settlement Amount'].replace(r',', '', regex=True)
        
        return wts
    except Exception as e:
        logging.exception('Error in formatWTSFile')
        raise
