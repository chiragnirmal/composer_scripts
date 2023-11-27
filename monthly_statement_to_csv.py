import camelot
import pandas as pd 

tables = camelot.read_pdf("2023-09.pdf",'1-end') # read all pages of the pdf

# Creating Empty DataFrame and Storing it in variable df 
maindf = pd.DataFrame() 
i=0

for table in tables:
    tabledf = table.df # get the dataframe version of the table
    if (tabledf.iloc[0,0] == 'Transaction'): #ensure this table contains transactions
        tabledf = tabledf.iloc[1:] #remove the first row that says transactions
        new_header = tabledf.iloc[0] #grab the second row for the header
        tabledf = tabledf[1:] #take the data less the header row
        tabledf.columns = new_header #set the header row as the df header
        tabledf['Trade Date'] = tabledf['Entry Type'].replace(' Trade Entry','',regex=True) # fix data
        tabledf['Entry Type'] = tabledf['Entry Type'].str[-11:]
        print(tabledf) #print table for debugging
        if (i==0):
            maindf = tabledf
        else:
            maindf = pd.concat([maindf, tabledf], ignore_index=True)
        i=i+1

maindf.to_csv("2023-09.csv")
