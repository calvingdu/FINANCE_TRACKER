import pandas as pd 
import csv

def get_schema(file): 
    df = pd.read_csv(file)

    with open(file, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        rows = [
            row for row in csv_reader if any(field.strip() for field in row) 
            and not ("Account TypeAccount NumberTransaction DateCheque NumberDescription 1Description 2CAD$USD$" in ''.join(row))
            ]

    df = pd.DataFrame(rows)
    print(df)

rbc_file = '/Users/calvindu/School/Projects/Statement_Data/csv96541.csv'
#get_schema(rbc_file)

directory = '/Users/calvindu/School/Projects/Statement_Data/unprocessed/'
def make_df():
    df1 = pd.read_csv(directory + 'AMEX.csv')
    df2 = pd.read_csv(directory + 'RBC_CHECKING.csv')
    df3 = pd.read_csv(directory + 'BMO_CC.csv')
    df4 = pd.read_csv(directory + 'BMO_CHECKING.csv')

    df = pd.concat([df1,df2,df3,df4])
    df = df.drop('Unnamed: 0', axis =1)

    df.to_csv(directory+'full.csv')

#make_df()

def read_cibc():
    df = pd.read_csv(directory+'cibc_1.csv')
    print(df.info())

read_cibc()