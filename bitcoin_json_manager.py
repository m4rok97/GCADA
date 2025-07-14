import os
import pickle
import networkx as nx
from bitcoin_json_reader import BitcoinJsonReader
from datetime import datetime


class BitcoinJsonManager(object):
    """ A class used for manage the data from bitcoin in json format. """

    def __init__(self, files_folder_path):
        """ Initialize a BitCoinJsonManager."""
        self.files_folder_path = files_folder_path
        self.json_reader = BitcoinJsonReader()
        self.addresses_graph = nx.Graph()
        self.users_graph = nx.Graph()
        self.address_user_dict = {}
        self.connected_components_list = []

    def parse_iso(self, ts: str) -> datetime:
        """
        Parse the given ISO 8601 timestamp string into a datetime object.
        """
        # e.g. "2012-06-19T04:29:41+0000"
        # Python's %z expects +HHMM or -HHMM
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")

    def create_addresses_relation_network_from_json_data(self, start_date=None, end_date=None):
        """
        Create the addresses relation network from the json files in the given folder path.
        """
        print('='*30)
        print('Analyzing files')
        total = len(os.listdir(self.files_folder_path))
        current = 1
        for filename in os.listdir(self.files_folder_path):
            print('Current ', current, ' / Total ', total)
            self.load_addresses_relation_network(
                os.path.join(self.files_folder_path, filename), start_date, end_date)
            current += 1

    def load_addresses_relation_network(self, file_path, start_date=None, end_date=None):
        """
        Load the addresses relation network from the given json file path.
        :param graph: The graph to load the addresses relation network into.
        :param json_reader: The json reader to read the json file.
        :param file_path: The json file path.
        """

        start_dt = self.parse_iso(start_date) if start_date else None
        end_dt = self.parse_iso(end_date) if end_date else None

        # reading the json file
        self.json_reader.read_block_file(file_path)

        # reading all transactions
        for transaction in self.json_reader.get_json_transaction_list():
            transaction_time = transaction['time']
            tx_dt = self.parse_iso(transaction_time)
            # date range check
            if start_dt and tx_dt < start_dt:
                continue
            if end_dt and tx_dt > end_dt:
                continue

            # date range check

            # analyze only the confirmed transactions
            if self.json_reader.is_confirmed_transaction(transaction) and \
                    not self.json_reader.is_coinbase_transaction(transaction):
                inputs = transaction['inputs']
                inputs_len = len(inputs)

                # check if the first node address is in the graph
                if inputs[0]['address'] and not (inputs[0]['address'] in self.addresses_graph.nodes):
                    # add the node in case it is not in the graph
                    self.addresses_graph.add_node(inputs[0]['address'])

                # iterate over the transaction inputs
                for i in range(0, inputs_len - 1):
                    # check if the node is in the graph
                    node_address = inputs[i]['address']
                    node_neighbor_address = inputs[i + 1]['address']

                    # check if the neighbor is in the graph the node is not checked because it was the neighbor
                    # from the previous iteration
                    if node_neighbor_address and not (node_neighbor_address in self.addresses_graph.nodes):
                        self.addresses_graph.add_node(node_neighbor_address)

                    # if there is not an edge between the two nodes then add it
                    if node_neighbor_address and not self.addresses_graph.has_edge(node_address, node_neighbor_address):
                        self.addresses_graph.add_edge(
                            node_address, node_neighbor_address)

                # iterate over the outputs, because some addresses can be only outputs in the time-span
                for output in transaction['outputs']:
                    output_address = output['address']
                    if output_address and output_address not in self.addresses_graph.nodes:
                        self.addresses_graph.add_node(output_address)

        # closing the block file
        self.json_reader.close_block_file()

    def get_list_of_connected_components(self, graph):
        """
        Get the list of connected components in the graph.
        :return: The list of connected components in the graph.
        """
        return list(nx.connected_components(graph))

    def create_address_user_dictionary(self, connected_components_list):
        """
        Create the address user dictionary from the connected components list.
        """
        result = {}
        cc_count = len(connected_components_list)
        for cc_index in range(0, cc_count):
            for addr in connected_components_list[cc_index]:
                result[addr] = cc_index

        self.address_user_dict = result

    def save_address_user_dict_to_file(self, file_path):
        """
        Save the address user dictionary to a file.
        :param file_path: The file path to save the dictionary to.
        """
        with open(file_path, 'wb') as file_handle:
            pickle.dump(self.address_user_dict, file_handle,
                        pickle.HIGHEST_PROTOCOL)

    def read_address_user_dict_from_file(self, file_path):
        """
        Read the address user dictionary from a file.
        :param file_path: The file path to read the dictionary from.
        """
        with open(file_path, 'rb') as file_handle:
            self.address_user_dict = pickle.load(file_handle)

    def load_user_network_from_json_data(self, file_path, address_user_dict, start_date=None, end_date=None):

        start_dt = self.parse_iso(start_date) if start_date else None
        end_dt = self.parse_iso(end_date) if end_date else None

        # reading the json file
        self.json_reader.read_block_file(file_path)

        # reading all transactions
        for transaction in self.json_reader.get_json_transaction_list():
            transaction_time = transaction['time']
            tx_dt = self.parse_iso(transaction_time)
            # date range check
            if start_dt and tx_dt <= start_dt:
                continue
            if end_dt and tx_dt >= end_dt:
                continue

            # analyze only the confirmed transactions
            if self.json_reader.is_confirmed_transaction(transaction) and \
                    not self.json_reader.is_coinbase_transaction(transaction):

                # getting the user_if from the input address
                if transaction['inputs'][0]['address']:
                    from_user = address_user_dict[transaction['inputs']
                                                  [0]['address']]

                    # check if the user is already in the graph and add it if it is not
                    if from_user not in self.users_graph.nodes:
                        self.users_graph.add_node(from_user)

                    for output in transaction['outputs']:
                        if output['address'] and address_user_dict[output['address']]:
                            to_user = address_user_dict[output['address']]

                            # check that the transaction is not a change transaction
                            if to_user != from_user:
                                # check if to_user is in the graph and add it if it is not
                                if to_user not in self.users_graph.nodes:
                                    self.users_graph.add_node(to_user)

                                # check if there is an edge between this two users
                                if self.users_graph.has_edge(from_user, to_user):
                                    # if there is an edge, increase the total transaction value and the transaction counter
                                    edge_data = self.users_graph[from_user][to_user]
                                    edge_data['value'] += output['value']
                                    edge_data['count'] += 1
                                else:
                                    # if the transaction is not in G then add it
                                    temp_dictionary = {
                                        'value': output['value'], 'count': 1}
                                    self.users_graph.add_edge(from_user, to_user,
                                                              **temp_dictionary)

        # closing the json file
        self.json_reader.close_block_file()

    def create_user_network_from_json_data(self, start_date=None, end_date=None):
        print('=' * 30)
        print('Analyzing files')

        total = len(os.listdir(self.files_folder_path))
        current = 1
        for filename in os.listdir(self.files_folder_path):
            print('Current ', current, ' / Total ', total)
            self.load_user_network_from_json_data(
                os.path.join(self.files_folder_path, filename), self.address_user_dict, start_date, end_date)
            current += 1

    def create_user_network_from_json_data_from_file(self, file_path):
        """
        Create the user network from the json data in the given file path.
        :param file_path: The json file path.
        """
        self.load_user_network_from_json_data(
            file_path, self.address_user_dict, start_date=None, end_date=None)

    def save_user_addresses_and_user_network_data_for_year(self, year, start_date=None, end_date=None):
        self.create_addresses_relation_network_from_json_data(
            start_date=start_date, end_date=end_date)
        print(self.addresses_graph)

        end_string = ''

        if start_date or end_date:
            end_string = f'{year}_{start_date}_{end_date}'
        else:
            end_string = str(year)

        # Save the addresses relation network to a file
        with open(f'datasets/bitcoin/{year}/addresses_relation_network_{end_string}.gpickle', 'wb') as file_handle:
            pickle.dump(self.users_graph, file_handle,
                        pickle.HIGHEST_PROTOCOL)

            self.connected_components_list = self.get_list_of_connected_components(
                self.addresses_graph)

            self.create_address_user_dictionary(
                self.connected_components_list)

            self.save_address_user_dict_to_file(
                f'datasets/bitcoin/{year}/addresses_user_dict_{end_string}.pickle')

            # From here can be executed the code to create the user network

            # Load the address user dictionary from the file
            self.read_address_user_dict_from_file(
                f'datasets/bitcoin/{year}/addresses_user_dict_{end_string}.pickle')

            # Create the user network
            self.create_user_network_from_json_data(
                start_date=start_date, end_date=end_date)

            # Save the user network to a file
            with open(f'datasets/bitcoin/{year}/user_network_{end_string}.gpickle', 'wb') as file_handle:
                pickle.dump(self.users_graph, file_handle,
                            pickle.HIGHEST_PROTOCOL)

            # Load the user network from a file
            with open(f'datasets/bitcoin/{year}/user_network_{end_string}.gpickle', 'rb') as file_handle:
                self.users_graph = pickle.load(file_handle)

            # Print the user network
            print(self.users_graph)


if __name__ == '__main__':
    year = 2012
    json_manager = BitcoinJsonManager(f'datasets/bitcoin/{year}/blocks')
    # json_manager.save_user_addresses_and_user_network_data_for_year(year)
    json_manager.save_user_addresses_and_user_network_data_for_year(
        year, '2012-06-01T04:29:41+0000', '2012-07-01T04:29:41+0000')

    # json_manager.create_addresses_relation_network_from_json_data()
    # print(json_manager.addresses_graph)

    # # Save the addresses relation network to a file
    # with open('datasets/bitcoin/2013/addresses_relation_network_2013.gpickle', 'wb') as file_handle:
    #     pickle.dump(json_manager.users_graph, file_handle,
    #                 pickle.HIGHEST_PROTOCOL)

    # connected_components_list = json_manager.get_list_of_connected_components(
    #     json_manager.addresses_graph)

    # json_manager.create_address_user_dictionary(connected_components_list)

    # json_manager.save_address_user_dict_to_file(
    #     'datasets/bitcoin/2013/addresses_user_dict_2013.pickle')

    # # From here can be executed the code to create the user network

    # # Load the address user dictionary from the file
    # json_manager.read_address_user_dict_from_file(
    #     'datasets/bitcoin/2013/addresses_user_dict_2013.pickle')

    # # Create the user network
    # json_manager.create_user_network_from_json_data()

    # # Save the user network to a file
    # with open('datasets/bitcoin/2013/user_network_2013.gpickle', 'wb') as file_handle:
    #     pickle.dump(json_manager.users_graph, file_handle,
    #                 pickle.HIGHEST_PROTOCOL)

    # # Load the user network from a file
    # with open('datasets/bitcoin/2013/user_network_2013.gpickle', 'rb') as file_handle:
    #     user_network = pickle.load(file_handle)

    # # Print the user network
    # print(user_network)

    # json_manager.create_user_network_from_json_data_f
