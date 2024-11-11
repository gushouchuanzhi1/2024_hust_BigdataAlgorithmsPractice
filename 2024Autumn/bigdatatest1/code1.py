import networkx as nx
from collections import defaultdict
import time

def calculate_modularity(G, communities):
    """计算给定图 G 和社区划分的模块度 Q"""
    m = G.size(weight='weight')
    Q = 0.0
    for community in communities.values():
        internal_weight = sum(G[u][v].get('weight', 1) for u in community for v in community if G.has_edge(u, v))
        degree_sum = sum(G.degree(n, weight='weight') for n in community)
        Q += internal_weight / (2 * m) - (degree_sum / (2 * m)) ** 2
    return Q

def initialize_communities(G):
    return {node: {node} for node in G.nodes()}

def modularity_gain(G, node, community, community_weight, total_weight):
    node_degree = G.degree(node, weight='weight')
    community_degree_sum = sum(G.degree(n, weight='weight') for n in community)
    links_to_community = sum(G[node][nbr].get('weight', 1) for nbr in community if G.has_edge(node, nbr))
    delta_Q = (links_to_community - (node_degree * community_degree_sum) / (2 * total_weight)) / (2 * total_weight)
    return delta_Q

def louvain_algorithm(G, target_communities=5):
    total_weight = G.size(weight='weight')
    communities = initialize_communities(G)
    node_to_community = {node: node for node in G.nodes()}

    while True:
        improved = False
        for node in G.nodes():
            original_community = node_to_community[node]
            best_community = original_community
            best_increase = 0

            communities[original_community].remove(node)
            neighbor_communities = set(node_to_community[neighbor] for neighbor in G.neighbors(node))
            for community in neighbor_communities:
                increase = modularity_gain(G, node, communities[community], total_weight, total_weight)
                if increase > best_increase:
                    best_increase = increase
                    best_community = community

            if best_community != original_community:
                node_to_community[node] = best_community
                communities[best_community].add(node)
                improved = True
            else:
                communities[original_community].add(node)

        if not improved or len(communities) <= target_communities:
            break

        new_G = nx.Graph()
        new_node_to_community = {}
        for community, nodes in communities.items():
            new_node = community
            new_G.add_node(new_node)
            new_node_to_community[new_node] = nodes
            for node in nodes:
                for neighbor in G.neighbors(node):
                    neighbor_community = node_to_community[neighbor]
                    weight = G[node][neighbor].get('weight', 1)
                    if new_G.has_edge(new_node, neighbor_community):
                        new_G[new_node][neighbor_community]['weight'] += weight
                    else:
                        new_G.add_edge(new_node, neighbor_community, weight=weight)

        G = new_G
        communities = new_node_to_community
        node_to_community = {node: community for community, nodes in communities.items() for node in nodes}

    return {community: nodes for community, nodes in communities.items() if nodes}

# 初始化无向图
G = nx.Graph()

# 读取文件并加载图结构
with open("Cit-HepPh.txt", "r") as file:
    for line in file:
        if line.startswith("#"):
            continue
        from_node, to_node = map(int, line.strip().split())
        G.add_edge(from_node, to_node)

# 获取图的信息
graph_info = f"图的节点数: {G.number_of_nodes()}\n图的边数: {G.number_of_edges()}\n"

# 记录程序开始时间
start_time = time.time()

# 应用 Louvain 算法进行社区划分，指定目标社区数为 5
communities = louvain_algorithm(G, target_communities=5)

# 记录程序结束时间
end_time = time.time()

# 计算程序运行时间
execution_time = f"程序运行时间: {end_time - start_time:.2f} 秒\n"

# 准备要写入的内容
result_content = graph_info + "最终社区划分：\n"
for community, nodes in communities.items():
    result_content += f"社区 {community}: 包含节点 {nodes}\n"

result_content += execution_time

# 将结果写入文件 result.txt
with open("code1ans.txt", "w") as result_file:
    result_file.write(result_content)

print("结果已保存到 code1ans.txt 文件中")
