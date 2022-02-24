# Block_Analysis_Tool

## Introduction
- The Block Analysis Tool is a tool used to detect sybil behavior on the blockchain. The tool is comprised of 3 programs. The first is the _Create_Graphs_and_Sets.py_ program. This takes as input a .csv file that has at least the following 3 attributes for each input:  
  - **from** - The sending address for a specific transaction
  - **to** - The receiving address for a specific transaction
  - **blockNumber** - The block number for which you want to start the investigation of the **from** account's prior history. 
  
- The outputs to this program are 3 as well (_See Dataset Documentation for more details_):
  - Money_Trace_Adk.pkl
  - Set.pkl
  - Edge_List.csv 
  - New_Ancestor_Accounts.csv
  - New_Active_Accounts.csv

- The next program is the _Account_Networks.py_ program. This generates images of the network of input accounts and ancestral connections. It allows you to view the connections at varying levels of depth. It creates charts up to the 3rd level of depth in the graphs, because beyond that it usually becomes incomprehensible. 

![Round_7_5000Blocks_Depth_test1](https://user-images.githubusercontent.com/56660047/154171569-6a4571bc-b950-442e-90d5-f7572dd70969.png)

    Legend:
      - Purple -> Input Account
      - Black -> Ancestor of 3 or more Input Accounts
      - Pink -> Ancestor of 2 or less Input Accounts
      - Green -> Labeled exchange in the Known_Nodes.csv
 
- The 3rd program, _Generate_Features_and_Predict.py_, generates features for a rule based classifier. The specific features and methodology are outlined in the dataset documentation. The output will be a list of accounts with a suspicion level between 1 and 4. There may be multiple outputs for each account, because there can be multiple instances that warrant suspicion when looking into the account's history. 

## How to use
1. Run the _Create_Graphs_and_Sets.py_ program. In order to run this program, it requires you to initiallize the following variables:
  - **root** - Give the root path to where you want to store your files 
  - **csv_file** - The input accounts with the from, to, and blockNumber attributes per row
  - **API_key** - This needs to be an Etherscan API key
  - **infura_url** - This is a url for access to the infura node
  - **Window** - This indicates how many blocks back in the history of an account you want to investigate. 5000 is the recommneded starting value. The length of time it takes to terminate exponentially the further back you go.
  - **Depth** - This indicates the maximum depth of the graphs you create. It is not recommended to go beyond 3 for the purposes of the classifier.
  - **Large_wallet** - This indicates how many transactions it takes upon a query to label the queried account a large wallet. This is done, because if you did not exclude these highly active accounts the length of time to terminate would increase a lot. The accounts linked to an active account are not further queried and the graph does not branch further from this instance. 50 is the recommended starting value, because increasing it again causes the program to take much longer to terminate. 

2. Run the _Generate_Features_and_Predict.py_ program. In order to run this, you have to initialize two variables from the previous program:
  - **root** - Give the root path to where you want to store your files 
  - **API_key** - This needs to be an Etherscan API key
 
3. (Optional) Run the _Account_Networks.py_ to visualize the ancestral network for your input data as shown in the image above. Before running the program, you should create the file paths: 
  - **root**\networks\images\pngs
  - **root**\networks\adjacencies\pngs

## Output Data

_Almost all variables come from the blockchain. The following is an index of newly generated variables for the purpose of or as-a-result of the program. Attributes that are not specified in this sheet come from the Etherscan query directly. A summary of these attributes can be found here._

**Edge_List:**
Each row in the Edge List is an edge in one of the graphs generated from an input account. These edges will always have a non-zero transfer of funds from an ancestor node to an input account or from an ancestor node to another ancestor node. All graphs generated for all input accounts are contained in this DataFrame.

-	Contributor/Contrib_Number – These two variables make up a unique identifier for each instance of an account input into the program. Contrib_Number is the index in the original list and Contributor is the wallet address. You can have multiple instances of the same Contributor with different wallet addresses. 
-	Depth – Indicates the depth in the graph for a given edge. Depth 1 indicates an edge between the input account and the first ancestor. Depth 2 indicates an edge between the first ancestor (parent) and the second ancestor (grandparent). 
-	Hash_from_to – This is a hash of the ‘from’, ‘to’, and ‘hash’ variables. This serves as a unique identifier since sometimes multiple sums of money can be sent in a single transaction. 

**Edge_List_w_Bursts:**
-	This dataset is generated by starting with the Edge_List dataset. Many attributes from the original set are stripped, but new ones are added. 
-	The following features are generated by pulling all outgoing transactions from the sending (‘from’) account in each edge between blockNumber – XXXX/2 and blockNumber + XXXX/2. The following features are then generated from this set. 
  - XXXX_Out – This is the number of outgoing transactions on the specified interval
  - XXXX_Out_U – This is the number of unique transactions on the same interval
  - XXXX_Out_Child – This is the number of unique transactions that are sent towards input accounts or other ancestors (Across all graphs). 
  - XXXX_Out_Child_Prop – (XXXX_Out_Child/XXXX_Out)
  - XXXX_Out_U_Child_Prop – (XXXX_Out_Child/XXXX_Out_U)
-	Value_from_gas = gas * gasPrice
-	Cumulative_value_from_gas = cumulativeGasUsed * gasPrice

**Money_Trace_Adj**
-	This is a data structure that contains an adjacency matrix for each input account. 
-	The data structure is of the form:
  - Key: Value …  
  - ‘to’: All other transaction data -> See here
-	These adjacency matrices are merged to for the Sets data structure for analyzing ancestor accounts at each depth. 

**Sets**

    Data Structure:
      [Depth] -> Level of Depth
        [Ancestor] -> Ancestors to input accounts
          [Gitcoin Contritor(s)] -> Input accounts that descend from the ancestor
            [[[Transaction Amount, Currency, Receiver, Block Number, transaction hash (Ancestor -> Receiver)],...,...]] -> Transaction info that made the ancestor


-	Sets is a dictionary where the key values are the depth level. Within each depth, there is a set of ancestors that became ancestors at this specific level of depth. These ancestors will not repeat in subsequent levels of depth unless it becomes an ancestor of a greater number of contributors from the prior level to the next. 






