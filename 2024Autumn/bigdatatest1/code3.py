import time
from collections import defaultdict
from itertools import chain
import numpy as np


class Graph:
    def __init__(self, n_nodes):
        self.adj_list = [[] for _ in range(n_nodes)]
        self.node_weights = np.zeros(n_nodes)  # 用于存储每个节点的度数

    def add_edge(self, u, v, weight=1):
        self.adj_list[u].append((v, weight))
        self.adj_list[v].append((u, weight))
        self.node_weights[u] += weight
        self.node_weights[v] += weight

    def neighbors(self, node):
        return self.adj_list[node]

    def degree(self, node):
        return self.node_weights[node]

    def size(self):
        return sum(self.node_weights) / 2  # 总边权重


def calculate_modularity(G, communities):
    m = G.size()
    Q = 0.0
    for community in communities.values():
        internal_weight = sum(
            w for u in community for v, w in G.neighbors(u) if v in community
        )
        degree_sum = sum(G.degree(n) for n in community)
        Q += internal_weight / (2 * m) - (degree_sum / (2 * m)) ** 2
    return Q


def initialize_communities(G, n_nodes):
    return {node: {node} for node in range(n_nodes)}


def modularity_gain(G, node, community, community_degree_sum, total_weight):
    node_degree = G.degree(node)
    links_to_community = sum(w for v, w in G.neighbors(node) if v in community)
    delta_Q = (links_to_community - (node_degree * community_degree_sum) / (2 * total_weight)) / (2 * total_weight)
    return delta_Q


def louvain_algorithm(G, n_nodes, target_communities=5):
    total_weight = G.size()
    communities = initialize_communities(G, n_nodes)
    node_to_community = {node: node for node in range(n_nodes)}
    community_degree_sum = {node: G.degree(node) for node in range(n_nodes)}

    while True:
        improved = False
        for node in range(n_nodes):
            original_community = node_to_community[node]
            best_community = original_community
            best_increase = 0

            communities[original_community].remove(node)
            community_degree_sum[original_community] -= G.degree(node)

            neighbor_communities = {node_to_community[neighbor] for neighbor, _ in G.neighbors(node)}
            for community in neighbor_communities:
                increase = modularity_gain(G, node, communities[community], community_degree_sum[community], total_weight)
                if increase > best_increase:
                    best_increase = increase
                    best_community = community

            if best_community != original_community:
                node_to_community[node] = best_community
                communities[best_community].add(node)
                community_degree_sum[best_community] += G.degree(node)
                improved = True
            else:
                communities[original_community].add(node)
                community_degree_sum[original_community] += G.degree(node)

        if not improved or len(communities) <= target_communities:
            break

        # 构建压缩的新图
        new_G = Graph(len(communities))
        new_node_to_community = {}
        for community, nodes in communities.items():
            new_node = community
            new_node_to_community[new_node] = nodes
            for node in nodes:
                for neighbor, weight in G.neighbors(node):
                    neighbor_community = node_to_community[neighbor]
                    # 检查是否存在边
                    edge_found = False
                    for idx, (existing_edge, existing_weight) in enumerate(new_G.adj_list[new_node]):
                        if existing_edge == neighbor_community:
                            # 更新权重
                            new_G.adj_list[new_node][idx] = (existing_edge, existing_weight + weight)
                            edge_found = True
                            break
                    if not edge_found:
                        # 添加新边
                        new_G.adj_list[new_node].append((neighbor_community, weight))

        G = new_G
        communities = new_node_to_community
        node_to_community = {node: community for community, nodes in communities.items() for node in nodes}

    return {community: nodes for community, nodes in communities.items() if nodes}


# 初始化图并加载数据
with open("Cit-HepPh.txt", "r") as file:
    nodes = set()
    edges = []
    for line in file:
        if line.startswith("#"):
            continue
        from_node, to_node = map(int, line.strip().split())
        edges.append((from_node, to_node))
        nodes.update([from_node, to_node])

n_nodes = max(nodes) + 1
G = Graph(n_nodes)
for u, v in edges:
    G.add_edge(u, v)

start_time = time.time()
communities = louvain_algorithm(G, n_nodes, target_communities=5)
end_time = time.time()

print("start\n")

# 输出结果
result_content = f"节点数: {n_nodes}, 边数: {len(edges)}\n"
for community, nodes in communities.items():
    result_content += f"社区 {community}: 包含节点 {nodes}\n"
result_content += f"运行时间: {end_time - start_time:.2f} 秒\n"

with open("code3ans.txt", "w") as result_file:
    result_file.write(result_content)
print("结果已保存到 code3ans.txt 文件中")
