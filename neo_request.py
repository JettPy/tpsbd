from py2neo import Graph


def request():
    graph_db = Graph('bolt://localhost:7687', auth=('neo4j', '123456'))
    response = graph_db.run('''
        MATCH (p:Purchase)-[r]->()
        WITH p.customer AS customer_fio, r.price AS price
        RETURN customer_fio, SUM(price) AS total_price
        ORDER BY total_price DESC LIMIT 1
    ''')
    while response.forward():
        print(response.current)


if __name__ == '__main__':
    request()
