import pandas as pd
import numpy as np
import sys
from tqdm import tqdm
import pickle
from datetime import datetime
from web3 import Web3

from Utils import Extract_Most_Suspicious_Bursts, Generate_Burst_Features, Additional_Features_and_Clean
from config import root, API_key

#Load in blockchain transactions
Edge_List = pd.read_csv(root + 'outputs/Edge_List.csv').reset_index()

print('Generate Burst Features:')
Edge_List_w_Bursts = Generate_Burst_Features(Edge_List, API_key)
Additional_Features_and_Clean(Edge_List_w_Bursts)
print('Complete')
print('')

print('Generate Burst Features:')
Suspicious_Accounts = Extract_Most_Suspicious_Bursts(Edge_List_w_Bursts)
print('Complete')
print('')

Edge_List_w_Bursts.to_csv(root + 'outputs/Edge_List_w_Bursts.csv')
Suspicious_Accounts.to_csv(root + 'outputs/Suspicious_Accounts.csv')


