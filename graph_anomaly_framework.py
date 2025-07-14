import os
import pickle
import numpy as np
import networkx as nx

from anomaly_detector import AnomalyDetector
from community_detector import CommunityDetector
from utils import (
    lap_score,
    feature_ranking,
    spectral_feature_selection,
    construct_community_affinity,
)


class GraphAnomalyFramework:
    def __init__(self, graph, community_cache_path=None):
        self.graph = graph
        self.communities = None
        self.anomalies = None
        self.attributes = None
        self.community_cache_path = community_cache_path
        self.dataset_name = None

    def _compute_neighbor_metrics(self, attributes=None):
        if not attributes:
            return

        work_attributes = attributes.copy()

        for attr in work_attributes:
            if attr in self.graph.nodes[next(iter(self.graph.nodes()))]:
                print(
                    f"Attribute '{attr}' already exists in graph nodes. Skipping computation.")
                work_attributes.remove(attr)

        if not work_attributes:
            print("No new attributes to compute. Exiting.")
            return

        mapping = {}
        for idx, comm in enumerate(self.communities):
            for node in comm:
                mapping[node] = idx
        for node in self.graph.nodes():
            if 'degree' in work_attributes:
                self.graph.nodes[node]['degree'] = self.graph.degree(node)

            c = None
            neighbors = None
            counts = None
            inner = None
            outer = None
            if 'innerCommunityEdgesAmount' in work_attributes:
                c = mapping.get(node, None)
                neighbors = list(self.graph.neighbors(node))
                counts = [sum(1 for nbr in neighbors if mapping.get(nbr) == i)
                          for i in range(len(self.communities))]
                inner = counts[c] if c is not None and c < len(counts) else 0
                self.graph.nodes[node]['innerCommunityEdgesAmount'] = inner
            if 'outerCommunityEdgesAmount' in work_attributes:
                if not neighbors:
                    neighbors = list(self.graph.neighbors(node))
                if not counts:
                    counts = [sum(1 for nbr in neighbors if mapping.get(nbr) == i)
                              for i in range(len(self.communities))]
                if not outer:
                    outer = sum(counts) - inner
                self.graph.nodes[node]['outerCommunityEdgesAmount'] = outer

            if 'neighborsCommunityVector' in work_attributes:
                if not neighbors:
                    neighbors = list(self.graph.neighbors(node))
                if not counts:
                    counts = [sum(1 for nbr in neighbors if mapping.get(nbr) == i)
                              for i in range(len(self.communities))]
                self.graph.nodes[node]['neighborsCommunityVector'] = counts
            if 'community' in work_attributes:
                if not c:
                    c = mapping.get(node, None)
                self.graph.nodes[node]['community'] = c

    def _compute_neighbor_metrics_spark(self, attributes=None, sc=None):
        """Compute node neighbor metrics using PySpark."""
        if not attributes:
            return

        work_attributes = attributes.copy()

        for attr in list(work_attributes):
            if attr in self.graph.nodes[next(iter(self.graph.nodes()))]:
                print(
                    f"Attribute '{attr}' already exists in graph nodes. Skipping computation.")
                work_attributes.remove(attr)

        if not work_attributes:
            print("No new attributes to compute. Exiting.")
            return

        from pyspark import SparkContext

        sc = sc or SparkContext.getOrCreate()
        graph_b = sc.broadcast(self.graph)
        mapping = {node: idx for idx, comm in enumerate(
            self.communities) for node in comm}
        mapping_b = sc.broadcast(mapping)
        comm_len_b = sc.broadcast(len(self.communities))

        def process(node):
            g = graph_b.value
            mp = mapping_b.value
            num_comms = comm_len_b.value
            res = {}
            neighbors = None
            counts = None
            inner = 0

            if 'degree' in work_attributes:
                res['degree'] = g.degree(node)

            if any(a in work_attributes for a in ['innerCommunityEdgesAmount',
                                                  'outerCommunityEdgesAmount',
                                                  'neighborsCommunityVector']):
                neighbors = list(g.neighbors(node))
                counts = [sum(1 for nbr in neighbors if mp.get(nbr) == i)
                          for i in range(num_comms)]

            if 'innerCommunityEdgesAmount' in work_attributes:
                c = mp.get(node, None)
                inner = counts[c] if c is not None and c < len(counts) else 0
                res['innerCommunityEdgesAmount'] = inner
            if 'outerCommunityEdgesAmount' in work_attributes:
                outer = sum(counts) - inner if counts is not None else 0
                res['outerCommunityEdgesAmount'] = outer
            if 'neighborsCommunityVector' in work_attributes:
                res['neighborsCommunityVector'] = counts if counts is not None else []
            if 'community' in work_attributes:
                res['community'] = mp.get(node, None)
            return node, res

        nodes_rdd = sc.parallelize(list(self.graph.nodes()))
        results = nodes_rdd.map(process).collect()
        for node, vals in results:
            self.graph.nodes[node].update(vals)

    def get_nodes_attributes(self):
        nodes = list(self.graph.nodes())
        if not nodes:
            return []
        sample = self.graph.nodes[nodes[0]]
        return [k for k in sample.keys() if k != 'community']

    def _select_attributes(self, base_attrs, percentile=0.1, method="laplacian"):
        fnames = []
        first = list(self.graph.nodes())[0]
        for attr in base_attrs:
            val = self.graph.nodes[first].get(attr)
            if isinstance(val, (int, float)):
                fnames.append(attr)
            elif hasattr(val, '__iter__'):
                try:
                    vec = list(val)
                    for i in range(len(vec)):
                        fnames.append(f"{attr}_{i}")
                except:
                    pass
        nodes = list(self.graph.nodes())
        X = np.zeros((len(nodes), len(fnames)))
        for i, node in enumerate(nodes):
            for j, fname in enumerate(fnames):
                if '_' in fname:
                    base, idx = fname.rsplit('_', 1)
                    vec = self.graph.nodes[node].get(base, [])
                    X[i, j] = vec[int(idx)] if idx.isdigit() and int(
                        idx) < len(vec) else 0
                else:
                    X[i, j] = self.graph.nodes[node].get(fname, 0)
        if method == "laplacian":
            W = construct_community_affinity(nodes, self.communities)
            scores = lap_score(X, W=W)
            idxs = feature_ranking(scores)
        elif method == "spectral":
            idxs = spectral_feature_selection(X)
        else:
            raise ValueError(f"Unknown feature selection method: {method}")
        top = max(1, int(len(idxs) * percentile))
        sel = idxs[:top]
        self.attributes = [fnames[k] for k in sel]
        print(f"Selected via {method.title()}:", self.attributes)

    def _select_attributes_spark(
        self, base_attrs, percentile=0.1, method="laplacian", sc=None
    ):
        """Select attributes using a feature selection method via Spark."""
        from pyspark import SparkContext

        sc = sc or SparkContext.getOrCreate()
        fnames = []
        first = list(self.graph.nodes())[0]
        for attr in base_attrs:
            val = self.graph.nodes[first].get(attr)
            if isinstance(val, (int, float)):
                fnames.append(attr)
            elif hasattr(val, '__iter__'):
                try:
                    vec = list(val)
                    for i in range(len(vec)):
                        fnames.append(f"{attr}_{i}")
                except Exception:
                    pass

        graph_b = sc.broadcast(self.graph)
        fnames_b = sc.broadcast(fnames)

        def extract(node):
            g = graph_b.value
            names = fnames_b.value
            row = []
            for fname in names:
                if '_' in fname:
                    base, idx = fname.rsplit('_', 1)
                    vec = g.nodes[node].get(base, [])
                    row.append(vec[int(idx)] if idx.isdigit()
                               and int(idx) < len(vec) else 0)
                else:
                    row.append(g.nodes[node].get(fname, 0))
            return row

        nodes_rdd = sc.parallelize(list(self.graph.nodes()))
        rows = nodes_rdd.map(extract).collect()
        X = np.array(rows)
        if method == "laplacian":
            W = construct_community_affinity(
                list(self.graph.nodes()), self.communities)
            scores = lap_score(X, W=W)
            idxs = feature_ranking(scores)
        elif method == "spectral":
            idxs = spectral_feature_selection(X)
        else:
            raise ValueError(f"Unknown feature selection method: {method}")
        top = max(1, int(len(idxs) * percentile))
        sel = idxs[:top]
        self.attributes = [fnames[k] for k in sel]
        print(f"Selected via {method.title()} (Spark):", self.attributes)

    def run(
        self,
        comm_method="louvain",
        anomaly_method="degree",
        threshold=0.6,
        attributes=None,
        feature_selection=None,
        percentile=0.1,
        use_spark=False,
        spark_context=None,
    ):

        if self.community_cache_path and os.path.exists(self.community_cache_path):
            with open(self.community_cache_path, 'rb') as f:
                self.communities = pickle.load(f)
            print(f"Loaded {len(self.communities)} communities from cache.")
        else:
            # detect fresh
            cd = CommunityDetector(self.graph)
            self.communities = cd.detect_communities(method=comm_method)
            if self.community_cache_path:
                with open(self.community_cache_path, 'wb') as f:
                    pickle.dump(self.communities, f)
                print(f"Saved {len(self.communities)} communities to cache.")

        # ——— 1) Compute neighbor metrics ———
        # This is a costly operation, but only needs to be done once
        # for each community detection run
        if use_spark:
            self._compute_neighbor_metrics_spark(
                attributes=attributes, sc=spark_context)
        else:
            self._compute_neighbor_metrics(attributes=attributes)

        # ——— 2) Determine attributes for Glance ———
        if feature_selection in {"laplacian", "spectral"}:
            base = attributes or self.get_nodes_attributes()
            if use_spark:
                self._select_attributes_spark(
                    base, percentile, method=feature_selection, sc=spark_context
                )
            else:
                self._select_attributes(
                    base, percentile, method=feature_selection)
        else:
            self.attributes = attributes or self.get_nodes_attributes()

        # ——— 3) Run anomaly detection ———
        if use_spark:
            from spark_anomaly_detector import SparkAnomalyDetector
            ad = SparkAnomalyDetector(
                self.graph, self.communities, self.attributes,
                sc=spark_context)
        else:
            ad = AnomalyDetector(self.graph, self.communities, self.attributes)

        if anomaly_method == 'degree':
            self.anomalies = ad.detect_degree_anomalies(threshold)
        elif anomaly_method == 'within_communities':
            self.anomalies = ad.detect_within_communities(threshold)
        elif anomaly_method == 'CADA':
            self.anomalies = ad.detect_CADA()
        elif anomaly_method == 'pseudo_CADA':
            self.anomalies = ad.detect_pseudo_CADA()
        elif anomaly_method == 'glance':
            self.anomalies = ad.detect_glance(threshold)
        else:
            raise ValueError(f"Unknown anomaly method: {anomaly_method}")

    def save_results(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        with open(os.path.join(path, 'anomalies.pkl'), 'wb') as f:
            pickle.dump(self.anomalies, f)
        # with open(os.path.join(path, 'communities.pkl'), 'wb') as f:
        #     pickle.dump(self.communities, f)
        print(f"Results saved to {path}")

    def save_communities_cache(self, comm_method='louvain'):
        """
        Detect communities and save to cache file.
        """
        if not self.community_cache_path:
            raise ValueError("community_cache_path not set.")
        cd = CommunityDetector(self.graph)
        self.communities = cd.detect_communities(method=comm_method)
        os.makedirs(os.path.dirname(self.community_cache_path), exist_ok=True)
        with open(self.community_cache_path, 'wb') as f:
            pickle.dump(self.communities, f)
        print(
            f"Saved {len(self.communities)} communities to cache at {self.community_cache_path}.")

    def save_anomalies_to_json(self, path):
        """
        Save anomalies to a JSON file.
        """
        import json
        if not self.anomalies:
            raise ValueError("No anomalies detected. Run 'run' method first.")
        with open(path, 'w') as f:
            json.dump(self.anomalies, f, indent=4)
        print(f"Anomalies saved to {path}")
