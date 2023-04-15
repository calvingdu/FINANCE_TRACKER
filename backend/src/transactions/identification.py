import csv 
import pandas as pd

def identify_bmo_cc(file, directory):
    with open(directory + file, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        if any('Following data is valid' in ''.join(row) for row in csv_reader) and any('Item #Card #Transaction DatePosting DateTransaction AmountDescription' in ''.join(row) for row in csv_reader):  
            return True

def identify_bmo_checking(file, directory):
    with open(directory + file, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        if any('Following data is valid' in ''.join(row) for row in csv_reader) and any('First Bank CardTransaction TypeDate Posted Transaction AmountDescription' in ''.join(row) for row in csv_reader):
            return True

def identify_rbc_checking(file, directory):
    with open(directory + file, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        if any("Account TypeAccount NumberTransaction DateCheque NumberDescription 1Description 2CAD$USD$" in ''.join(row) for row in csv_reader):
            return True

def identify_amex(file, directory):
    df = pd.read_csv(directory + file)
    if len(df.columns) == 6 and df.iloc[:, 2].dtype == 'float64':
        return True

def identify_cibc_checking(file, directory):
    df = pd.read_csv(directory + file)
    if len(df.columns) == 4 and df.iloc[:,2].dtype == 'float64' and df.iloc[:,3].dtype == 'float64' and df.iloc[:,1].dtype == 'object':
        return True

def identify_cibc_cc(file, directory):
    df = pd.read_csv(directory + file)
    if len(df.columns) == 5 and df.iloc[:,2].dtype == 'float64' and df.iloc[:,3].dtype == 'float64' and df.iloc[:,1].dtype == 'object':
        return True
