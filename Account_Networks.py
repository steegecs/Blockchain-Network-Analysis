import pandas as pd
import pickle
import matplotlib.pyplot as plt
import networkx as nx

root = "C:/Users/15138/Documents/Github Programs/"

image_dest = root + "networks/images/"
adj_dest = root + "networks/adjacencies/"

Transactions = pd.read_csv(root + "raw_data/tokenomics/Gitcoin_Donations_R7.csv").reset_index()
with open(root + 'raw_data/Sets.pkl', 'rb') as file:
    Sets = pickle.load(file)

Gitcoin_Donors = []
[Gitcoin_Donors.append(Transactions.iloc[i]['from']) for i in range(len(Transactions))]


Known_Large_Nodes = pd.read_csv(root + "outputs\Known_Nodes.csv")
Known_Nodes = {}

for i in range(len(Known_Large_Nodes)):
    Known_Nodes[Known_Large_Nodes.iloc[i]['Addresses']] = Known_Large_Nodes.iloc[i]['Wallet Name']


def get_transaction_graph(Gitcoin_Donors, Sets, Known_Nodes) -> nx.Graph:
    Depth = 1

    # Iterating through depths to create a chart for each depth level
    for i in range(1, 4):

        G = nx.MultiDiGraph()  # This needs to be a MultiDirectionGraph

        color_map = []  # A color map for the nodes in the graph
        # node_size = [] #Might use if I want to weight the nodes with something like account size or activity

        '''
        The subsequent ordering of how you store nodes and their colors matters.
        There is a certain preference for labeling nodes as green vs. black for instance.
        '''

        # Start by appending Gitcoin Donors
        for addr in Gitcoin_Donors:
            if addr not in G:
                G.add_node(addr)
                color_map.append("purple")

        # Next we want to track ancestors and how many donors they are ancestors of
        ancestors = {}
        for j in range(1, i + 1):
            for ancestor, receivers in Sets[j].items():
                if ancestor not in ancestors or len(Sets[j][ancestor]) > ancestors[ancestor]:
                    ancestors[ancestor] = len(Sets[j][ancestor])

        for ancestor, value in ancestors.items():
            if ancestor not in G:
                G.add_node(ancestor)
                # If this is some kind of known/vetted exchange, pool, contract, etc.
                if ancestor in Known_Nodes:
                    color_map.append("green")
                # If the ancestor is an ancestor of 3 or less donors
                elif value < 4:
                    color_map.append("pink")
                else:
                    color_map.append("black")

        # This is to keep track of edges as to not add the exact same ones twice.
        # This could happen given the way the data is stored
        transaction_hash = set()

        for j in range(1, i + 1):
            for ancestor, receivers in Sets[j].items():
                for receiver, transactions in receivers.items():
                    for transaction in transactions:
                        if transaction[3] not in transaction_hash:
                            # Ancestor giving money to a gitcoin contributor or some gitcoin contributors' parent
                            G.add_edge(ancestor, transaction[2])

        print(f"nEdges:        {G.number_of_edges()}")

        graph_f = nx.draw_spring

        # Create Graph
        graph_f(
            G,
            node_color=color_map,
            node_size=20,
            edge_color="lightgrey",
            linewidths=0.0001,
            arrowsize=6,
        )

        plt.savefig(image_dest + "pngs/Depth_" + str(Depth) + ".png")
        plt.savefig(image_dest + "pngs/Depth_" + str(Depth) + ".svg", format="svg")
        nx.write_adjlist(G, adj_dest + "pngs/Depth_" + str(Depth) + ".txt")

        Depth = Depth + 1

get_transaction_graph(Gitcoin_Donors, Sets, Known_Nodes)
