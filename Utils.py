import pandas as pd
import numpy as np
import requests
from web3 import Web3
from tqdm import tqdm
from copy import deepcopy


def Expand_Graph(API_key, Transaction, Receiving_Addresses, Very_Active_Account, Money_Trace_Adj, Edge_List, Window, Depth, Large_Wallet, i):
    # Transaction - Transaction that was recorded on the blockchain
    # Very_Active_Account - Wallets that were extremely active - Had a high summative in/out degree
    # All_Data            - All blockchain data associated with transactions stored in the Adjacency list
    # AdjList             - Adjacency list to store accounts and transactions as nodes and edges in a directed graph
    # Key: 'to' Ethereum Address
    # Value: 'from' Ethereum Address, a hash of 'hash'+'from'+'to'. (Transaction Hash, 'from', and 'to' Ethereum Addresses)

    # Cur_Adj = {} #Stores the adjacency list that stems from a Sender of a Transaction
    Transaction_dict = Transaction.to_dict()
    Cur_Money_Trace_Adj = {}
    Cur_Edge_List = pd.DataFrame()

    Hash = set()  # Stores all custom unique transaction hashed

    Transaction_dict['depth'] = 0 #Keep track of the depth in the adjacency list
    Queue = [Transaction_dict] #Breadth First Search Queue for searching transactions lineable to initial sender
    Hash.add(Transaction['hash_from_to'])
    Start_block = int(Transaction['blockNumber']) - Window  # Start block so that we only pull blocks withing a specific range for the whole search

    while len(Queue) > 0:

        current = Queue.pop(0)

        # Pulling all transactions os a parent account withing a block time range
        Eth = requests.get(
            f"https://api.etherscan.io/api?module=account&action=txlist&address={current['from']}&startblock={Start_block}&endblock={current['blockNumber']}&apikey={API_key}").json()
        ERC20 = requests.get(
            f"https://api.etherscan.io/api?module=account&action=tokentx&address={current['from']}&startblock={Start_block}&endblock={current['blockNumber']}&apikey={API_key}").json()

        data = []

        # 'Status' checks if the transaciton is successful
        if Eth['status'] == '1': data = data + Eth['result']
        if ERC20['status'] == '1': data = data + ERC20['result']

        # This section of code is to label and store very active account so they will not be search and drastically slow the search
        if len(data) > Large_Wallet:  # Replace with a rate at some point
            if current['from'] not in Very_Active_Account or Very_Active_Account[current['from']] < len(data):
                Very_Active_Account[current['from']] = len(data)

        else:
            # Next_Queue   - Used to store incoming transactions to be added to Queue after compression
            Next_Queue = pd.DataFrame()

            # Iterate through all transactions associated with parent account
            for result in data:
                result['hash_from_to'] = hash(result['hash'] + result['from'] + result['to'])  # create hash for storage and reference
                result['depth'] = current['depth'] + 1
                result['Contributor'] = Transaction['from']
                result['Contrib_Number'] = i

                if result['to'] not in Receiving_Addresses and result['hash_from_to'] not in Hash:
                    # if result['hash_from_to'] not in Hash: Switch back to this at some point

                    # Add incoming transactions to that are sending money and are not from known very large wallets to Next_Queue
                    if result['to'] == current['from'] and result['value'] != '0':

                        if result['to'] not in Cur_Money_Trace_Adj:
                            Cur_Money_Trace_Adj[result['to']] = [result]

                        else:
                            Cur_Money_Trace_Adj[result['to']].append(result)

                        result_df = pd.DataFrame([result])
                        Edge_List = pd.concat([Edge_List, result_df], ignore_index=True)

                        if result['depth'] <= Depth:  # This specifies how deep of a tree we want to be allowed to create
                            Next_Queue = pd.concat([Next_Queue, result_df], ignore_index=True)

                    Hash.add(result['hash_from_to'])

            # This section is for adding our Next_Queue to the Queue
            if Next_Queue.empty == False:
                # Filter out duplicate addresses to queue and choose the one with the latest block number to reduce redundancy
                Next_Queue.sort_values(by=['from', 'blockNumber'], ascending=False)
                Next_Queue = Next_Queue.groupby('from').head(1)

                [Queue.append(Next_Queue.iloc[i]) for i in range(len(Next_Queue))]

                # AdjList.append(Cur_Adj)

    Money_Trace_Adj = Money_Trace_Adj.append(Cur_Money_Trace_Adj)
    # Edge_List = Edge_List.append(Cur_Edge_List, ignore_index=True)

    return Edge_List


def Get_Sets(AdjList, Transactions, Sets):
    # AdjList       :This is a list of adjacency lists. One for each initial Transaction
    # Transactions  :Pass the original transactions used to create graphs
    # Sets          :Each set withing each depth of the graphs indicate all new or updated connections
    # that contribution accounts have to ancestor accounts

    #Get the list of addresses used to generate graphs
    Accounts = []
    [Accounts.append(Transactions.iloc[i]['from']) for i in range(len(Transactions))]

    # This is used to indicate that a new set has been
    # made at an address or updated from the previous depth
    rank_update = {}
    pageRank = {}
    # Dictionary of Dictionaries
    # key:    Ancestor nodes of contributing accounts
    # value/key:  All initial accounts that may have received money from this account
    # value: list of lists - [[Transaction value, Currency]]

    items = True  # This is used as an indicator to indicate that there is more to search

    Depth = 1  # This keeps track of the depth in the graphs that is being searched

    Addresses = []  # Keep track of the donor the money is flowing to
    [Addresses.append([Accounts[i]]) for i in range(len(Accounts))]

    while items == True:
        items = False

        # Iterate through all contributing accounts list of respective parents/ancestors at current depth
        for i in range(len(Addresses)):

            # This is used to update 'Addresses' with their parents
            Next = []

            # Iterate through all parents/ancestors
            for j in range(len(Addresses[i])):

                # Checks if the adjacency list has parents of current parent/ancestor
                if Addresses[i][j] in AdjList[i]:

                    # Iterate through
                    for Adj in AdjList[i][Addresses[i][j]]:
                        if 'tokenSymbol' not in Adj.keys(): Adj['tokenSymbol'] = 'Eth'
                        # If parent not already seen then do...
                        if Adj['from'] not in pageRank.keys():
                            pageRank[Adj['from']] = {
                                i: [[Adj['value'], Adj['tokenSymbol'], Adj['to'], Adj['blockNumber'], Adj['hash']]]}
                            rank_update[Adj['from']] = 1
                            Next.append(Adj['from'])
                            items = True

                        # Else if this contribution accounts parent/ancestor has
                        # not already been recorded/searched then do...
                        elif i not in pageRank[Adj['from']]:
                            pageRank[Adj['from']][i] = [
                                [Adj['value'], Adj['tokenSymbol'], Adj['to'], Adj['blockNumber'], Adj['hash']]]
                            rank_update[Adj['from']] = 1
                            Next.append(Adj['from'])
                            items = True

                        else:
                            pageRank[Adj['from']][i].append(
                                [Adj['value'], Adj['tokenSymbol'], Adj['to'], Adj['blockNumber'], Adj['hash']])

            # Append all new parents from previous set of parents/ancestors to the next depth's addresses
            Addresses[i] = Next

        cur_sets = {}  # Created to then be appended to 'Sets'

        # Iterate through parent/ancestor accounts in 'pageRank'
        for address in pageRank.keys():

            # Only move forward with sets of a certain size and only add to set of
            # sets if the pageRank for an address is new or it has been updated
            if len(pageRank[address].keys()) < 1 or rank_update[address] == 0:
                continue
            else:
                cur_sets[address] = deepcopy(pageRank[address])

        Sets[Depth] = cur_sets
        Depth = Depth + 1

        # Reset rank_update for parent/ancestor accounts to 0
        for address in rank_update.keys():
            rank_update[address] = 0
'''
Sets: Data Structure
[Depth]
    [Ancestor]
        [Initial Accounts]
            [[[Transaction Amount, Currency, Receiver, Block Number, transaction hash (Ancestor -> Receiver)],...,...]]
'''

def Create_Set_Feats(Sets):
    # Search through all depths
    for depth, ancestors in Sets.items():
        # Search through all parent/ancestor accounts
        for ancestor, contributors in ancestors.items():
            # Stores the transaction count and amounts for each currency per parent/ancestor
            from_dictionary = {}
            # Seach though all transactions per contributor of transaction from a parent/ancestor node
            for contributor, transactions in contributors.items():
                # Stores the transaction count and amounts for each currency per contributor from an parent/ancestor
                to_dictionary = {}
                for transaction in transactions:
                    if transaction[1] not in to_dictionary:
                        to_dictionary[transaction[1]] = [int(transaction[0]), 1]  # [Currency: [value, count]]
                    else:
                        to_dictionary[transaction[1]][0] = to_dictionary[transaction[1]][0] + int(transaction[0])
                        to_dictionary[transaction[1]][1] = to_dictionary[transaction[1]][1] + 1

                Sets[depth][ancestor][contributor] = [transactions, to_dictionary]

                for key in to_dictionary.keys():
                    if key not in from_dictionary:
                        from_dictionary[key] = [to_dictionary[key][0], to_dictionary[key][1]]
                    else:
                        from_dictionary[key][0] = from_dictionary[key][0] + to_dictionary[key][0]
                        from_dictionary[key][1] = from_dictionary[key][1] + to_dictionary[key][1]

            Sets[depth][ancestor]['All'] = from_dictionary

    '''
    Sets: Data Structure

    #Sets[Depth]
        [Ancestor]
            [Initial Accounts]
                [[[Transaction Amount, Currency, Receiver, Block Number, transaction hash (Ancestor -> Receiver)],...,...], {Currency: [Total Amount, Count]}]
    '''


def Pull_Bursts(i, unique_DF, row, Ancestor_to_Child, API_key):

    Burst_Data = [] #This is a data structure to ancestor data based on transaction behavior at various intervals
                    
    Eth = requests.get(f"https://api.etherscan.io/api?module=account&action=txlist&address={row['from']}&startblock={int(row['blockNumber']) - 5760/2}&endblock={int(row['blockNumber']) + 5760/2}&apikey={API_key}").json()
    ERC20 = requests.get(f"https://api.etherscan.io/api?module=account&action=tokentx&address={row['from']}&startblock={int(row['blockNumber']) - 5760/2}&endblock={int(row['blockNumber']) + 5760/2}&apikey={API_key}").json()
                
    data = pd.DataFrame()
                
    Burst_Frames = [5760, 2880, 1440, 240] #Week, Day, 12 HRs, 6 HRs, 1 HR
        
    #'Status' checks if the transaciton is successful
    if Eth['status'] == '1': data = data.append(Eth['result'])
    if ERC20['status'] == '1': data = data.append(ERC20['result'])
                
    data.sort_values(by=['blockNumber'])
                                           
    #All outgoing transactions that indicate a flow of money from the ancestor
    Out = data[(data['from'] == row['from']) & (data['value'] != 0)] 
                
    for frame in Burst_Frames: 
                    
        #Cut the data to be within the next burst interval (blockNumber)
        Out = Out[(int(row['blockNumber']) - frame/2) <= Out['blockNumber'].apply(int)]
        Out = Out[Out['blockNumber'].apply(int) <= (int(row['blockNumber']) + frame/2)]
                    
        #The set of all ougoing transactions
        Out_Set = set(Out['to'])
                    
        Children_Set = set()
                    
        #Create a set of all outgoing transactions that head toward an ancestor or contributing account
        for item in Out_Set:
            if row['from'] in Ancestor_to_Child: 
                if item in Ancestor_to_Child[row['from']]:
                    Children_Set.add(item)
        
        unique_DF.at[i, str(frame)+'_Out'] = len(Out)  
        unique_DF.at[i, str(frame)+'_Out_U'] = len(Out_Set)  
        unique_DF.at[i, str(frame)+'_Out_Child'] = len(Children_Set)  
        
        if len(Out) > 0:
            unique_DF.at[i, str(frame)+'_Out_Child_Prop'] = len(Children_Set)/len(Out)
            unique_DF.at[i, str(frame)+'_Out_U_Child_Prop'] = len(Children_Set)/len(Out_Set)   
    


def Generate_Burst_Features(Edge_List, API_key): 

    #Get unique transactions in the Edge List for generating burst features efficiently
    DF = Edge_List.sort_values(by=['hash_from_to'])
    unique_DF = DF.drop_duplicates(subset='hash_from_to', keep='first', inplace=False).reset_index()

    #Create a dictionary that keeps a mapping of all ancestors to their children
    Ancestor_to_Child = {} 
    for i in range(len(unique_DF)): 
        if unique_DF.iloc[i]['from'] not in Ancestor_to_Child.keys(): 
            Ancestor_to_Child[unique_DF.iloc[i]['from']] = [unique_DF.iloc[i]['to']]
        else:
            Ancestor_to_Child[unique_DF.iloc[i]['from']].append(unique_DF.iloc[i]['to'])
    for k, v in Ancestor_to_Child.items(): 
        Ancestor_to_Child[k] = set(v)

    #Iterate through all unique edges to generate burst features
    for i in tqdm(range(len(unique_DF))):
        while True:
            try: 
                Pull_Bursts(i, unique_DF, unique_DF.iloc[i], Ancestor_to_Child, API_key)
                break
            except Exception as e:
                print(e)
                continue

    unique_DF = unique_DF[['hash_from_to', '5760_Out', '5760_Out_U', '5760_Out_Child', '5760_Out_Child_Prop', '5760_Out_U_Child_Prop', '2880_Out', '2880_Out_U', '2880_Out_Child', '2880_Out_Child_Prop', '2880_Out_U_Child_Prop', '1440_Out', '1440_Out_U', '1440_Out_Child', '1440_Out_Child_Prop', '1440_Out_U_Child_Prop', '240_Out', '240_Out_U', '240_Out_Child', '240_Out_Child_Prop', '240_Out_U_Child_Prop']].sort_values(by='hash_from_to')
    DF = DF.merge(unique_DF, left_on='hash_from_to', right_on='hash_from_to')

    return DF

def Additional_Features_and_Clean(DF):
    #Genarate additional features and simplify the dataframe (drop some stuff)
    DF['Value_from_gas'] = DF['gas'].apply(int) * DF['gasPrice'].apply(int)
    DF['Cumulative_value_from_gas'] = DF['gasPrice'].apply(int) * DF['cumulativeGasUsed'].apply(int)
    DF['tokenSymbol'] = DF['tokenSymbol'].fillna('ETH')
    DF['contractAddress'] = DF['contractAddress'].replace(r'^\s*$', np.nan, regex=True)
    DF['Contract'] = DF['contractAddress'].fillna('Yes')
    #DF['Contract'][DF['Contract'] != 'Yes'] = 'No'
    DF = DF.drop(['contractAddress', 'cumulativeGasUsed', 'input', 'gas', 'gasPrice', 'gasUsed', 'confirmations', 'isError', 'txreceipt_status', 'timeStamp', 'nonce', 'hash', 'blockHash', 'transactionIndex', 'tokenDecimal', 'tokenName'], axis=1)


def Store_Active_Accounts(Very_Active_Account, w3, Byte_Code_Hash, root): 
    #The purpose of this section of code is to process the active wallets recorded when creating graphs.
    #We want to audit at least some of the previously unseen accounts to determine their nature
    Active_Wallets_List = []
    Active_Contracts_List = []
    Known_Nodes = pd.read_csv(root + "outputs/Known_Nodes.csv")

    #Store in the respective data structures initiallized above depending on if the account is a wallet or contract
    for key, value in Very_Active_Account.items():
        byte_code = w3.eth.get_code(Web3.toChecksumAddress(key))
        if byte_code != b'':      
            Active_Contracts_List.append([hash(byte_code), key, value])
            #Check and add to our hash(byte_code): byte_code mapping
            if hash(byte_code) not in Byte_Code_Hash: 
                Byte_Code_Hash[hash(byte_code)] = key
        else: 
            Active_Wallets_List.append([key, value])

    #Create Dataframes for our lists
    Active_Wallets = pd.DataFrame(Active_Wallets_List, columns = ['Address', 'Count'])
    Active_Contracts = pd.DataFrame(Active_Contracts_List, columns = ['Byte_Code', 'Address', 'Count'])

    #Sort the newly found active wallets and known nodes for merger with one another
    Active_Wallets.sort_values(by='Address', ascending=False)
    Known_Nodes.sort_values(by='Address', ascending=False)

    #Perform a left outer merge between the two and then store in a new datafile to manually analyze newly seen active wallets
    DF_Wallets = pd.merge(Active_Wallets, Known_Nodes['Address'], how='left', on='Address', indicator=True)
    DF_Wallets_left_only = DF_Wallets[DF_Wallets['_merge'] == 'left_only']

    #Sort the newly found active contracts and known for merger with one another
    Active_Contracts.sort_values(by='Byte_Code', ascending=False)
    Known_Nodes.sort_values(by='Byte_Code', ascending=False)

    #Perform a left outer merge between the two and then store in a new datafile to manually analyze newly seen active contracts
    DF_Contracts = pd.merge(Active_Contracts, Known_Nodes['Byte_Code'], how='left', on='Byte_Code', indicator=True)
    DF_Contracts_left_only = DF_Contracts[DF_Contracts['_merge'] == 'left_only']

    #Perform a left outer merge with instances of the previous merge where the byte code was contained in both
    DF_Contracts_both = DF_Contracts[DF_Contracts['_merge'] == 'both'].drop(['_merge'], axis=1)
    DF_Contracts_Addresses = pd.merge(DF_Contracts_both, Known_Nodes['Address'], how='left', on='Address', indicator=True)

    #Append instances to the Known_Nodes database where there was a matching byte code but a missing address in the known nodes
    #I do this because the contract is known and assumed to be audited. It is just another address
    Known_Nodes = Known_Nodes.append(DF_Contracts_Addresses)
    Known_Nodes.sort_values(by=['Byte_Code', 'Address'], ascending=False).to_csv(root + "outputs/Known_Nodes.csv")

    #Create a csv file based on previously unseen contracts and wallets to audit
    New_Active_Accounts = DF_Wallets_left_only.append(DF_Contracts_left_only).sort_values(by=['Byte_Code', 'Count'], ascending=False)
    New_Active_Accounts.to_csv(root + 'outputs/New_Active_Accounts.csv')

def Store_Ancestor_Accounts(Sets, w3, Byte_Code_Hash, root): 
    #The purpose of this section of code is to process the ancestors recorded when creating sets.
    #We want to audit at least some of the previously unseen accounts to determine their nature.
    Ancestor_Contracts_List = []
    Ancestor_Wallets_List = []
    Known_Nodes = pd.read_csv(root + "outputs/Known_Nodes.csv")

    #Store in the respective data structures initiallized above depending on if the ancestor is a wallet or contract
    #Note: ancestors may be recorded multiple times if they appear in multiple depths
    for depth, ancestors in tqdm(Sets.items()):
        for ancestor, contributors in tqdm(ancestors.items()): 
            #Just limiting to ancestors that are at least mildly suspicious
            if len(contributors) > 2: 
                byte_code = w3.eth.get_code(Web3.toChecksumAddress(ancestor))
                if byte_code != b'':      
                    Ancestor_Contracts_List.append([hash(byte_code), ancestor, depth, len(contributors)])
                    #Check and add to our hash(byte_code): byte_code mapping
                    if hash(byte_code) not in Byte_Code_Hash: 
                        Byte_Code_Hash[hash(byte_code)] = ancestor
                else: 
                    Ancestor_Wallets_List.append([ancestor, depth, len(contributors)])
                
    #Create Dataframes for our lists   
    Ancestor_Wallets = pd.DataFrame(Ancestor_Wallets_List, columns = ['Address', 'Depth', 'Contributor_Count'])
    Ancestor_Contracts = pd.DataFrame(Ancestor_Contracts_List, columns = ['Byte_Code', 'Address', 'Depth', 'Contributor_Count'])

    #Sort the newly found active wallets and known nodes for merger with one another
    Ancestor_Wallets.sort_values(by='Address', ascending=False)
    Known_Nodes.sort_values(by='Address', ascending=False)

    #Perform a left outer merge between the two and then store in a new datafile to manually analyze newly seen ancestor wallets
    DF_Wallets = pd.merge(Ancestor_Wallets, Known_Nodes['Address'], how='left', on='Address', indicator=True)
    DF_Wallets_left_only = DF_Wallets[DF_Wallets['_merge'] == 'left_only']

    #Sort the newly found active contracts and known for merger with one another
    Ancestor_Contracts.sort_values(by='Byte_Code', ascending=False)
    Known_Nodes.sort_values(by='Byte_Code', ascending=False)

    #Perform a left outer merge between the two and then store in a new datafile to manually analyze newly seen ancestor contracts
    DF_Contracts = pd.merge(Ancestor_Contracts, Known_Nodes['Byte_Code'], how='left', on='Byte_Code', indicator=True)
    DF_Contracts_left_only = DF_Contracts[DF_Contracts['_merge'] == 'left_only']

    #Perform a left outer merge with instances of the previous merge where the byte code was contained in both
    DF_Contracts_both = DF_Contracts[DF_Contracts['_merge'] == 'both'].drop(['_merge'], axis=1)
    DF_Contracts_Addresses = pd.merge(DF_Contracts_both, Known_Nodes['Address'], how='left', on='Address', indicator=True)

    #Append instances to the Known_Nodes database where there was a matching byte code but a missing address in the known nodes
    #I do this because the contract is known and assumed to be audited. It is just another address
    Known_Nodes = Known_Nodes.append(DF_Contracts_Addresses)
    Known_Nodes.sort_values(by=['Byte_Code', 'Address'], ascending=False).to_csv(root + "outputs\Known_Nodes.csv")

    #Create a csv file based on previously unseen contracts and wallets to audit
    New_Ancestor_Accounts = DF_Wallets_left_only.append(DF_Contracts_left_only).sort_values(by=['Byte_Code', 'Depth'], ascending=False)
    New_Ancestor_Accounts.to_csv(root + "outputs/New_Ancestor_Accounts.csv")


def Extract_Most_Suspicious_Bursts(DF):

    Suspicious_transactions = pd.DataFrame()
    Suspicious_Accounts = {}
    Suspicious_transactions = Suspicious_transactions.append(DF[((DF['5760_Out_Child'] > 1) & (DF['5760_Out_Child_Prop'] > 0.2))])
    Suspicious_transactions = Suspicious_transactions.append(DF[((DF['2880_Out_Child'] > 1) & (DF['2880_Out_Child_Prop'] > 0.2))])
    Suspicious_transactions = Suspicious_transactions.append(DF[((DF['1440_Out_Child'] > 1) & (DF['1440_Out_Child_Prop'] > 0.2))])
    Suspicious_transactions = Suspicious_transactions.append(DF[((DF['240_Out_Child']  > 1) & (DF['240_Out_Child_Prop']  > 0.2))])
    Suspicious_transactions = Suspicious_transactions.sort_values(by=['Contributor', 'depth']).drop_duplicates(subset='Contributor', keep='first', inplace=False)
    Suspicious_transactions['Suspicion_Level'] = 2 - Suspicious_transactions['depth']
    for i, row in Suspicious_transactions.iterrows():
        Suspicious_Accounts[row['Contributor']] = row.to_dict()

    Suspicious_transactions_1 = pd.DataFrame()
    Suspicious_transactions_1  = Suspicious_transactions_1.append(DF[((DF['5760_Out_Child'] > 1) & (DF['5760_Out_Child_Prop'] > 0.4))])
    Suspicious_transactions_1  = Suspicious_transactions_1.append(DF[((DF['2880_Out_Child'] > 1) & (DF['2880_Out_Child_Prop'] > 0.4))])
    Suspicious_transactions_1  = Suspicious_transactions_1.append(DF[((DF['1440_Out_Child'] > 1) & (DF['1440_Out_Child_Prop'] > 0.4))])
    Suspicious_transactions_1  = Suspicious_transactions_1.append(DF[((DF['240_Out_Child']  > 1) & (DF['240_Out_Child_Prop']  > 0.4))])
    Suspicious_transactions_1 = Suspicious_transactions_1.sort_values(by=['Contributor', 'depth']).drop_duplicates(subset='Contributor', keep='first', inplace=False)
    Suspicious_transactions_1['Suspicion_Level'] = 3 - Suspicious_transactions_1['depth']
    for i, row in Suspicious_transactions_1.iterrows():
        if row['Suspicion_Level'] > Suspicious_Accounts[row['Contributor']]['Suspicion_Level']:
            Suspicious_Accounts[row['Contributor']] = row

    Suspicious_transactions_2 = pd.DataFrame()
    Suspicious_transactions_2 = Suspicious_transactions_2.append(DF[((DF['5760_Out_Child'] > 1) & (DF['5760_Out_Child_Prop'] > 0.6))])
    Suspicious_transactions_2 = Suspicious_transactions_2.append(DF[((DF['2880_Out_Child'] > 1) & (DF['2880_Out_Child_Prop'] > 0.6))])
    Suspicious_transactions_2 = Suspicious_transactions_2.append(DF[((DF['1440_Out_Child'] > 1) & (DF['1440_Out_Child_Prop'] > 0.6))])
    Suspicious_transactions_2 = Suspicious_transactions_2.append(DF[((DF['240_Out_Child']  > 1) & (DF['240_Out_Child_Prop']  > 0.6))])
    Suspicious_transactions_2 = Suspicious_transactions_2.sort_values(by=['Contributor', 'depth']).drop_duplicates(subset='Contributor', keep='first', inplace=False)
    Suspicious_transactions_2['Suspicion_Level'] = 4 - Suspicious_transactions_2['depth']
    for i, row in Suspicious_transactions_2.iterrows():
        if row['Suspicion_Level'] > Suspicious_Accounts[row['Contributor']]['Suspicion_Level']:
            Suspicious_Accounts[row['Contributor']] = row

    Suspicious_transactions_3 = pd.DataFrame()
    Suspicious_transactions_3 = Suspicious_transactions_3.append(DF[((DF['5760_Out_Child'] > 2) & (DF['5760_Out_Child_Prop'] > 0.8))])
    Suspicious_transactions_3 = Suspicious_transactions_3.append(DF[((DF['2880_Out_Child'] > 2) & (DF['2880_Out_Child_Prop'] > 0.8))])
    Suspicious_transactions_3 = Suspicious_transactions_3.append(DF[((DF['1440_Out_Child'] > 2) & (DF['1440_Out_Child_Prop'] > 0.8))])
    Suspicious_transactions_3 = Suspicious_transactions_3.append(DF[((DF['240_Out_Child']  > 2) & (DF['240_Out_Child_Prop']  > 0.8))])
    Suspicious_transactions_3 = Suspicious_transactions_3.sort_values(by=['Contributor', 'depth']).drop_duplicates(subset='Contributor', keep='first', inplace=False)
    Suspicious_transactions_3['Suspicion_Level'] = 5 - Suspicious_transactions_3['depth']
    for i, row in Suspicious_transactions_3.iterrows():
        if row['Suspicion_Level'] > Suspicious_Accounts[row['Contributor']]['Suspicion_Level']:
            Suspicious_Accounts[row['Contributor']] = row

    DF = pd.DataFrame(Suspicious_Accounts.values())
    print(DF)    
    return DF
        