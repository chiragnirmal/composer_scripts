import camelot
import pandas as pd 

monthlyStatements = ["2023-01.pdf","2023-02.pdf","2023-03.pdf","2023-04.pdf","2023-05.pdf","2023-06.pdf","2023-07.pdf","2023-08.pdf","2023-09.pdf","2023-10.pdf"] # TODO: Update the list of pdfs to be processed here

# function to agreegate all the transaction at the symbol level
def aggDf(df, fileName):
    print("Summary of transactions found in {file_name}-agg.csv:".format(file_name = fileName))
    aggdf = df.groupby('Symbol').agg({'Quantity':'sum', 'Amount': 'sum', 'Trade Date' : 'max'}).round(2).sort_values(['Trade Date','Amount'],ascending=False)
    print(aggdf)
    aggdf.to_csv("{file_name}-agg.csv".format(file_name = fileName))
    return aggdf

# function to read all transactions from 1 pdf
def readPdf(fileName):
    print("Processing {file_name}".format(file_name = fileName))
    # Creating Empty DataFrame and Storing it in variable df 
    mainDf = pd.DataFrame() 
    i=0
    tables = camelot.read_pdf(fileName,'1-end') # read all pages of the pdf
    for table in tables:
        tableDf = table.df # get the dataframe version of the table
        if (tableDf.iloc[0,0] == 'Transaction'): #ensure this table contains transactions
            # remove \n & \t
            for col in tableDf.columns:
                tableDf[col] = tableDf[col].replace("[\n,\t]"," ", regex=True)
            tableDf = tableDf.iloc[1:] #remove the first row that says transactions
            new_header = tableDf.iloc[0] #grab the second row for the header
            new_header[1] = 'Entry Type' #fix headers
            new_header[2] = 'Side'       #fix headers
            new_header[3] = 'Symbol'     #fix headers
            tableDf = tableDf[1:] #take the data less the header row
            tableDf.columns = new_header #set the header row as the df header
            tableDf['Trade Date'] = pd.to_datetime(tableDf['Entry Type'].replace(' Trade Entry','',regex=True)) # fix date
            tableDf['Quantity'] = pd.to_numeric(tableDf['Quantity']) # fix quantity
            tableDf['Price'] = pd.to_numeric(tableDf['Price'].replace("[$, ]",'', regex=True)) # fix price
            tableDf['Amount'] = tableDf['Amount'].str.replace('$ --', '0') # fix amount
            tableDf['Amount'] = pd.to_numeric(tableDf['Amount'].replace("[$, ]",'', regex=True)) # fix amount
            tableDf['Entry Type'] = tableDf['Entry Type'].str[-11:]
            if (i==0):
                mainDf = tableDf
            else:
                mainDf = pd.concat([mainDf, tableDf], ignore_index=True)
            i=i+1
    print("All transactions found in {file_name}.cvs:".format(file_name = fileName))
    print(mainDf) #print table for debugging
    mainDf.to_csv("{file_name}.csv".format(file_name = fileName))
    aggDf(mainDf, fileName)
    return mainDf

# Read all transactions from all the pdf files and merge them into 1 dataframe
def readAllPdfAndAgg(monthlyStatements):
    # Creating Empty DataFrame and Storing it in variable df 
    allTransactionDf = pd.DataFrame() 
    # Read all transactions from all the pdf files and merge them into 1 dataframe
    for file in monthlyStatements:
        monthlyDf = readPdf(file)
        allTransactionDf = pd.concat([allTransactionDf, monthlyDf], ignore_index=True)

    # output all the combines list of all transactions into 1 file
    print("All transactions found in total")
    print(allTransactionDf)
    allTransactionDf.to_csv('all.csv')
    # agreegate the combines list of all transactions into 1 file
    aggDf(allTransactionDf, "all")

readAllPdfAndAgg(monthlyStatements)
print("All symbols with Amount <0 have a loss")
print("All symbols with Quanity =0 have been bought and sold fully")
print("All symbols with Quanity >0 have been bought not sold fully")
print("All symbols with Quanity <0 have been sold but there is corresponding buy transaction")
