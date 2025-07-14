import community.community_louvain as community_louvain
import networkx as nx
from networkx.algorithms import community as nx_comm


class CommunityDetector:
    def __init__(self, graph):
        self.graph = graph
        self.communities = []

    def detect_communities(self, method='louvain'):
        print(f"Detecting communities using {method} method...")
        if method == 'louvain':
            partition = community_louvain.best_partition(self.graph)
            comm_map = {}
            for node, cid in partition.items():
                comm_map.setdefault(cid, []).append(node)
            self.communities = list(comm_map.values())
        elif method == 'girvan_newman':
            comp = nx_comm.girvan_newman(self.graph)
            self.communities = [list(c) for c in next(comp)]
        else:
            raise ValueError(f"Unsupported community method: {method}")
        return self.communities
