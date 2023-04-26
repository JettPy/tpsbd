from elasticsearch import Elasticsearch
from py2neo import Graph, Node, Relationship, NodeMatcher
from constants import PRODUCT_INDEX, PURCHASE_INDEX


def init_neo_db():
    client = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
    graph_db = Graph('bolt://localhost:7687', auth=('neo4j', '123456'))
    graph_db.delete_all()
    products = client.search(index=PRODUCT_INDEX, size=1000)['hits']['hits']
    purchases = client.search(index=PURCHASE_INDEX, size=1000)['hits']['hits']
    for product in products:
        productNode = Node(
            'Product',
            id=product['_id'],
            product_name=product['_source']['product_name']
        )
        graph_db.create(productNode)
    
    matcher = NodeMatcher(graph_db).match('Product')
    
    for purchase in purchases:
        purchaseNode = Node(
            'Purchase',
            id=purchase['_id'],
            date=purchase['_source']['purchase_date'],
            customer=purchase['_source']['personal_data']
        )
        graph_db.create(purchaseNode)
        productNode = matcher.where(f"_.id = '{purchase['_source']['product_id']}'").first()
        NodeIncludeRelationship = Relationship(
            purchaseNode,
            'Include',
            productNode,
            amount=purchase['_source']['amount'],
            price=purchase['_source']['price']
        )
        graph_db.create(NodeIncludeRelationship)
        customerNode = graph_db.nodes.match(
            'Customer',
            id=purchase['_source']['customer_id'],
            name=purchase['_source']['personal_data']
        ).first()
        if customerNode is None:
            customerNode = Node(
                'Customer',
                id=purchase['_source']['customer_id'],
                name=purchase['_source']['personal_data']
            )
            graph_db.create(productNode)
        NodeMakeRelationship = Relationship(
            customerNode,
            'Make',
            purchaseNode
        )
        graph_db.create(NodeMakeRelationship)
    print('База данных neo4j c покупателями проинициализирована')


if __name__ == '__main__':
    init_neo_db()
