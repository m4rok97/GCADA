import numpy as np


class AnomalyDetector:
    def __init__(self, graph, communities=None, selected_attrs=None):
        self.graph = graph
        self.communities = communities or []
        self.selected_attrs = selected_attrs
        self.anomalies_scores = {}
        self.anomalies = {}

    @staticmethod
    def _get_average_difference(community, attribute):
        n = len(community)
        acc = 0.0
        for node_i in community:
            for node_j in community:
                acc += abs(node_i.get(attribute, 0) - node_j.get(attribute, 0))
        return acc / (n**2) if n > 0 else 0

    @staticmethod
    def _get_average_differences(community):
        diffs = {}
        if not community:
            return diffs
        for attribute in filter(lambda x: x not in ('nodeId', 'id', 'community'), community[0].keys()):
            diffs[attribute] = AnomalyDetector._get_average_difference(
                community, attribute)
        return diffs

    def detect_degree_anomalies(self, threshold=2.0):
        """Detect anomalies based on node degree."""
        print("Detecting degree anomalies...")
        degrees = dict(self.graph.degree())
        vals = np.array(list(degrees.values()))
        mean, std = vals.mean(), vals.std()
        self.anomalies = {n: d for n,
                          d in degrees.items() if abs(d-mean) > threshold*std}
        return self.anomalies

    def detect_within_communities(self, threshold=2.0):
        result = {}
        # use precomputed 'innerCommunityEdgesAmount' for each node
        for idx, comm in enumerate(self.communities):
            inner_vals = np.array([self.graph.nodes[n].get(
                'innerCommunityEdgesAmount', 0) for n in comm])
            if len(inner_vals) == 0:
                result[idx] = []
                continue
            mean, std = inner_vals.mean(), inner_vals.std()
            result[idx] = [n for n, val in zip(
                comm, inner_vals) if abs(val - mean) > threshold * std]
        self.anomalies = result
        return self.anomalies

    def detect_CADA(self):
        # use precomputed 'neighborsCommunityVector'
        scores = {}
        for comm in self.communities:
            for node in comm:
                counts = self.graph.nodes[node].get(
                    'neighborsCommunityVector', [])
                max_in = max(counts) if counts else 0
                scores[node] = sum(
                    c / max_in for c in counts) if max_in > 0 else 0
        self.anomalies = scores
        return self.anomalies

    def detect_pseudo_CADA(self):
        # use precomputed 'innerCommunityEdgesAmount' and 'outerCommunityEdgesAmount'
        scores = {}
        for comm in self.communities:
            for node in comm:
                inner = self.graph.nodes[node].get(
                    'innerCommunityEdgesAmount', 0)
                outer = self.graph.nodes[node].get(
                    'outerCommunityEdgesAmount', 0)
                scores[node] = outer / inner if inner > 0 else float('inf')
        self.anomalies = scores
        return self.anomalies

    def detect_glance(self, threshold=0.6):
        scores = {}
        for comm in self.communities:
            # prepare data dicts
            community_data = []
            for node in comm:
                attrs = dict(self.graph.nodes[node])
                attrs['nodeId'] = node
                # filter attributes for glance
                if self.selected_attrs:
                    attrs = {k: v for k, v in attrs.items(
                    ) if k in self.selected_attrs or k == 'nodeId'}
                community_data.append(attrs)
            # compute
            diffs = AnomalyDetector._get_average_differences(community_data)
            n = len(community_data)
            for nd in community_data:
                scores.setdefault(nd['nodeId'], 0)
                node_scores = []
                for attr, avg in diffs.items():
                    acc = sum(1 for other in community_data
                              if other['nodeId'] != nd['nodeId']
                              and abs(nd.get(attr, 0)-other.get(attr, 0)) > avg)
                    node_scores.append(acc/n if n > 0 else 0)
                scores[nd['nodeId']] = max(node_scores) if node_scores else 0
        self.anomalies_scores = scores
        self.anomalies = {n: s for n, s in scores.items() if s > threshold}
        return self.anomalies
