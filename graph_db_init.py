from elasticsearch import Elasticsearch
from py2neo import Graph, Node, Relationship
from tqdm import tqdm


client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
product_index = "product"
purchase_index = "purchase"

graph_db = Graph("bolt://localhost:7687", auth=("neo4j", "123"))
graph_db.delete_all()

products = client.search(index=product_index, size=1000)
purchases = client.search(index=purchase_index, size=1000)
for product in products:
    print(product)
print('\n=====================================================================================================\n')
for purchase in purchases:
    print(purchase)
c_c = 0
# for purchase in purchases['hits']['hits']:
#     try:
#         # create purchase node
#         purchase_node = Node(
#             "Purchase",
#             id=purchase['_source']['purchase_date'],
#             date=purchase['_source']['purchase_date']
#         )
#         graph_db.create(purchase_node)
#         # create customer node if needed
#         product_node = graph_db.nodes.match(
#             "Customer",
#             customerId = exercise['_source']['customer_id']
#         ).first()
#         if customerNode is None:
#             customerNode = Node(
#                 "Customer",
#                 customerId = exercise['_source']['customer_id'],
#                 info = exercise['_source']['personal_info']
#             )
#             graph_db.create(customerNode)
#         # Connect Customer and Exercise
#         connection = Relationship(
#             customerNode,
#             "Visit",
#             exerciseNode,
#         )
#         graph_db.create(connection)
#     except Exception as e:
#         print(e)
#         continue
#
#     for trainer in trainers['hits']['hits']:
#         try:
#             trainerNode = graph_db.nodes.match(
#                 "Trainer",
#                 trainerId = trainer['_id']
#             ).first()
#             if trainerNode is None:
#                 trainerNode = Node(
#                     "Trainer",
#                     trainerId = trainer['_id'],
#                     info = trainer['_source']['personal_info'],
#                     speciality = trainer['_source']['speciality'],
#                     experience = trainer['_source']['experience']
#                 )
#                 graph_db.create(trainerNode)
#             # connect Exercise and Trainer
#             if str(exercise['_source']['trainer_id']) == trainer['_id']:
#                 connection = Relationship(
#                     trainerNode,
#                     "Hold",
#                     exerciseNode,
#                     date=exercise['_source']['exercise_date']
#                 )
#                 graph_db.create(connection)
#
#         except Exception as e:
#             print(e)
#             continue
