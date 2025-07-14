from pyspark import SparkContext
from graph_anomaly_framework import GraphAnomalyFramework
from graph_manager import GraphManager
import time


if __name__ == '__main__':
    sc = SparkContext(appName='GraphAnomalySpark')
    start = time.perf_counter()
    dataset_name = 'disney_amazon'  # change as needed
    manager = GraphManager(dataset_name)
    graph = manager.load_graph()

    framework = GraphAnomalyFramework(graph,
                                      community_cache_path=f'cache/{dataset_name}_communities.pkl')
    framework.dataset_name = dataset_name

    anomaly_method = 'glance'
    framework.run(comm_method='louvain',
                  anomaly_method=anomaly_method,
                  use_spark=True,
                  feature_selection='laplacian',
                  spark_context=sc)

    sc.stop()

    end = time.perf_counter()
    print(f"Execution time (Spark): {end - start:.3f} seconds")

    framework.save_anomalies_to_json(
        f'anomalies_{dataset_name}_{anomaly_method}.json')
