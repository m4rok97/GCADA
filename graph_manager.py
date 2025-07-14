import os
import networkx as nx
import pickle
from bitcoin_json_reader import BitcoinJsonReader


class GraphManager:
    """ A class used to save and load graphs from various datasets. """

    DISNNEY_AMAZON_DATASET_PATH = 'datasets/disney_amazon/Disney.graphml'
    US_ELECTION_RETWEETS_DATASET_PATH = 'datasets/us_elections_2018_retweets/us_elections_2018_retweets.gpickle'
    BITCOIN_2012_DATASET_PATH = 'datasets/bitcoin/2012/user_network_2012.gpickle'
    BITCOIN_2013_DATASET_PATH = 'datasets/bitcoin/2013/user_network_2013.gpickle'
    BITCOIN_2012_REDUCED_DATASET_PATH = 'datasets/bitcoin/2012/user_network_2012_2012-06-01T04:29:41+0000_2012-07-01T04:29:41+0000.gpickle'

    def __init__(self, data_set_name):
        self.dataset_name = data_set_name

    def load_graph(self):

        if self.dataset_name == 'disney_amazon':
            return self.load_graph_from_graphml(self.DISNNEY_AMAZON_DATASET_PATH)
        elif self.dataset_name == 'us_election_retweets':
            return self.load_graph_from_gpickle(self.US_ELECTION_RETWEETS_DATASET_PATH)
        elif self.dataset_name == 'bitcoin_2012':
            return self.load_graph_from_gpickle(self.BITCOIN_2012_DATASET_PATH)
        elif self.dataset_name == 'bitcoin_2013':
            return self.load_graph_from_gpickle(self.BITCOIN_2013_DATASET_PATH)
        elif self.dataset_name == 'bitcoin_2012_reduced':
            return self.load_graph_from_gpickle(self.BITCOIN_2012_REDUCED_DATASET_PATH)

    def save_graph(self):
        if self.dataset_name == 'bitcoin_2012':
            return self.save_graph_to_gpickle(self.load_graph_from_gpickle(self.BITCOIN_2012_DATASET_PATH), 'bitcoin_2012.gpickle')
        elif self.dataset_name == 'bitcoin_2013':
            return self.save_graph_to_gpickle(self.load_graph_from_gpickle(self.BITCOIN_2013_DATASET_PATH), 'bitcoin_2013.gpickle')
        elif self.dataset_name == 'bitcoin_2012_reduced':
            return self.save_graph_to_gpickle(self.load_graph_from_gpickle(self.BITCOIN_2012_REDUCED_DATASET_PATH), 'bitcoin_2012_reduced.gpickle')

    def load_graph_from_graphml(self, file_path):
        return nx.read_graphml(file_path)

    def load_graph_from_gpickle(self, file_path):
        with open(file_path, 'rb') as f:
            return pickle.load(f)

    def load_graph_from_gml(self, file_path):
        return nx.read_gml(file_path)

    def save_graph_to_gpickle(self, graph, file_path):
        with open(file_path, 'wb') as f:
            pickle.dump(graph, f)


def get_list_of_connected_components(graph):
    return list(nx.connected_components(graph))
