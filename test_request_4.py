import json


def test():
    customer_name = ''
    product_name = ''
    max_total_price = 0
    with open('purchases.json') as file:
        purchases = json.load(file)
        for purchase in purchases:
            customer = purchase['body']['personal_data']
            product = purchase['body']['product_name']
            price = purchase['body']['price']
            if price > max_total_price:
                max_total_price = price
                customer_name = customer
                product_name = product
    print(f'Покупатель и товар с максимальной стоимостью: {customer_name} - {product_name} ({max_total_price})')


if __name__ == '__main__':
    test()