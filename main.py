import time
from graph_anomaly_framework import GraphAnomalyFramework
from graph_manager import GraphManager
from community_detector import CommunityDetector
import networkx as nx

if __name__ == '__main__':

    dataset_name = 'disney_amazon'  # Change this to the desired dataset name
    manager = GraphManager(dataset_name)
    graph = manager.load_graph()
    print(graph)

    framework = GraphAnomalyFramework(
        graph, community_cache_path=f'cache/{dataset_name}_communities.pkl')
    framework.dataset_name = dataset_name

    anomaly_method = 'glance'
    framework.run(comm_method='louvain',
                  anomaly_method=anomaly_method,
                  feature_selection='laplacian')

    print(
        f"Anomalies detected using {anomaly_method} method in dataset {dataset_name}:")

    framework.save_anomalies_to_json(
        f'anomalies_{dataset_name}_{anomaly_method}.json')
