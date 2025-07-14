from pyspark import SparkContext
from anomaly_detector import AnomalyDetector
import numpy as np


class SparkAnomalyDetector(AnomalyDetector):
    """Parallel anomaly detection using PySpark."""

    def __init__(self, graph, communities=None, selected_attrs=None, sc=None):
        super().__init__(graph, communities, selected_attrs)
        self.sc = sc or SparkContext.getOrCreate()
        # broadcast the graph for workers
        self.graph_b = self.sc.broadcast(self.graph)

    def detect_degree_anomalies(self, threshold=2.0):
        sc = self.sc
        nodes_rdd = sc.parallelize(list(self.graph.nodes()))
        graph_b = self.graph_b
        degrees_rdd = nodes_rdd.map(
            lambda n: (n, graph_b.value.degree(n)))
        stats = degrees_rdd.map(lambda x: x[1]).stats()
        mean = stats.mean()
        std = stats.stdev()
        result = degrees_rdd.filter(
            lambda x: abs(x[1] - mean) > threshold * std)
        self.anomalies = dict(result.collect())
        return self.anomalies

    def detect_within_communities(self, threshold=2.0):
        sc = self.sc
        comm_rdd = sc.parallelize(list(enumerate(self.communities)))
        graph_b = self.graph_b

        def process(data):
            idx, comm = data
            g = graph_b.value
            vals = np.array(
                [g.nodes[n].get('innerCommunityEdgesAmount', 0) for n in comm])
            if len(vals) == 0:
                return (idx, [])
            mean = float(vals.mean())
            std = float(vals.std())
            res = [n for n, val in zip(comm, vals) if abs(
                val - mean) > threshold * std]
            return (idx, res)

        result = dict(comm_rdd.map(process).collect())
        self.anomalies = result
        return self.anomalies

    def detect_CADA(self):
        sc = self.sc
        comm_rdd = sc.parallelize(self.communities)
        graph_b = self.graph_b

        def score_nodes(comm):
            g = graph_b.value
            local_scores = {}
            for node in comm:
                counts = g.nodes[node].get('neighborsCommunityVector', [])
                max_in = max(counts) if counts else 0
                local_scores[node] = sum(
                    c / max_in for c in counts) if max_in > 0 else 0
            return list(local_scores.items())

        scores = comm_rdd.flatMap(score_nodes).collect()
        self.anomalies = dict(scores)
        return self.anomalies

    def detect_pseudo_CADA(self):
        sc = self.sc
        comm_rdd = sc.parallelize(self.communities)
        graph_b = self.graph_b

        def score_nodes(comm):
            g = graph_b.value
            local_scores = {}
            for node in comm:
                inner = g.nodes[node].get('innerCommunityEdgesAmount', 0)
                outer = g.nodes[node].get('outerCommunityEdgesAmount', 0)
                local_scores[node] = outer / \
                    inner if inner > 0 else float('inf')
            return list(local_scores.items())

        scores = comm_rdd.flatMap(score_nodes).collect()
        self.anomalies = dict(scores)
        return self.anomalies

    def detect_glance(self, threshold=0.6):
        sc = self.sc
        comm_rdd = sc.parallelize(self.communities)
        graph_b = self.graph_b
        attrs_b = sc.broadcast(self.selected_attrs)

        def process(comm):
            g = graph_b.value
            attrs = attrs_b.value
            data = []
            for node in comm:
                nd_attrs = dict(g.nodes[node])
                nd_attrs['nodeId'] = node
                if attrs:
                    nd_attrs = {k: v for k, v in nd_attrs.items()
                                if k in attrs or k == 'nodeId'}
                data.append(nd_attrs)

            diffs = {}

            if data:
                # Determine which keys to process (exclude identifiers and data tag)
                attributes = [
                    key for key in data[0].keys()
                    if key not in ('nodeId', 'id', 'community')
                ]

                # Compute average absolute difference for each attribute
                for attribute in attributes:
                    n = len(data)
                    total_diff = 0.0

                    for node_i in data:
                        for node_j in data:
                            total_diff += abs(node_i.get(attribute, 0) -
                                              node_j.get(attribute, 0))

                    diffs[attribute] = total_diff / (n ** 2) if n > 0 else 0.0

            n = len(data)
            result = []
            for nd in data:
                node_scores = []
                for attr, avg in diffs.items():
                    acc = sum(
                        1 for other in data
                        if other['nodeId'] != nd['nodeId'] and abs(nd.get(attr, 0) - other.get(attr, 0)) > avg
                    )
                    node_scores.append(acc / n if n > 0 else 0)
                result.append(
                    (nd['nodeId'], max(node_scores) if node_scores else 0))
            return result

        scores = comm_rdd.flatMap(process).collect()
        score_dict = dict(scores)
        self.anomalies_scores = score_dict
        self.anomalies = {n: s for n, s in score_dict.items() if s > threshold}
        return self.anomalies
