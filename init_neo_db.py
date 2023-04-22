from elasticsearch import Elasticsearch
from py2neo import Graph, Node, Relationship

from constants import PRODUCT_INDEX, PURCHASE_INDEX


def init_neo_db():
    client = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
    graph_db = Graph('bolt://localhost:7687', auth=('neo4j', '123456'))
    graph_db.delete_all()
    products = client.search(index=PRODUCT_INDEX, size=1000)
    purchases = client.search(index=PURCHASE_INDEX, size=1000)
    # for product in products['hits']['hits']:
    #     try:
    #         product_node = Node(
    #             'Product',
    #             id=product['_id'],
    #             product_name=product['_source']['product_name']
    #         )
    #         graph_db.create(product_node)
    #     except Exception:
    #         continue
    
    for purchase in purchases['hits']['hits']:
        try:
            purchase_node = Node(
                'Purchase',
                id=purchase['_id'],
                date=purchase['_source']['purchase_date'],
                customer=purchase['_source']['personal_data']
            )
            graph_db.create(purchase_node)
            product_node = graph_db.nodes.match(
                'Product',
                product_id=purchase['_source']['product_id']
            ).first()
            if product_node is None:
                product_node = Node(
                    'Product',
                    id=purchase['_source']['product_id'],
                    product_name=purchase['_source']['product_name']
                )
                graph_db.create(product_node)
            connection = Relationship(
                purchase_node,
                'Include',
                product_node,
                amount=purchase['_source']['amount'],
                price=purchase['_source']['price']
            )
            graph_db.create(connection)
        except Exception:
            continue


if __name__ == '__main__':
    init_neo_db()
