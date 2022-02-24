import pandas as pd
import numpy as np
import sys
from tqdm import tqdm
import pickle
from web3 import Web3

from Utils import Expand_Graph, Get_Sets, Store_Active_Accounts, Store_Ancestor_Accounts
from config import root, csv_file, API_key, infura_url

#Recommended starting values.
Window = 5000
Depth = 3
Large_Wallet = 50

#Connect to Web3.py to Infura Node
w3 = Web3(Web3.HTTPProvider(infura_url))
if w3.isConnected(): 
    print('Connected to Infura Node:')
    print('')
else: 
    sys.exit('Check Infura Connection')

#Load in blockchain transactions
Transactions = pd.read_csv(root + "raw_data" + csv_file).reset_index()

#Extract all Receiving addresses
Receiving_Addresses = set(Transactions['to'].to_list())

# Data structures to keep track of active accounts, Adjacency Lists, and Tabular Edge List
Very_Active_Account = {}
Money_Trace_Adj = []
Edge_List = pd.DataFrame()

# Create subgraphs for each contributing account
print('Starting Subgraph Generation:')
for i in tqdm(range(0, len(Transactions))):
    while True:
        try:
            Edge_List = Expand_Graph(API_key, Transactions.iloc[i], Receiving_Addresses, Very_Active_Account, Money_Trace_Adj, Edge_List, Window, Depth, Large_Wallet, i)
            break
        except Exception as e:
            print(e)
            if len(Money_Trace_Adj) == i + 1: 
                Money_Trace_Adj.pop()
            Edge_List = Edge_List[Edge_List['Contrib_Number'] != i]
            continue
print('Subgraphs Generated')
print('')

print('Generating sets of common ancestors:')
#Extract all common ancestors with some relavant associated transaction data
Sets = {}
Get_Sets(Money_Trace_Adj, Transactions, Sets)
print('Complete')
print('')

#Store Active/Ancestor Accounts in a csv file for optional inspections
print('Storing Active and Ancestor Nodes:')
Byte_Code_Hash = {}
Store_Active_Accounts(Very_Active_Account, w3, Byte_Code_Hash, root)
Store_Ancestor_Accounts(Sets, w3, Byte_Code_Hash, root)
print('Complete')
print('')

with open(root + 'raw_data/Money_Trace_Adj.pkl', 'wb') as file:
    pickle.dump(Money_Trace_Adj, file)
with open(root  +'raw_data/Sets.pkl', 'wb') as file:
    pickle.dump(Sets, file)

Edge_List.to_csv(root + 'outputs/Edge_List.csv')



