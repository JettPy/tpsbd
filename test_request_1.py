import json


def test():
    products_per_month = {}
    with open('products.json') as file:
        products = json.load(file)
        for product in products:
            amount_of_sold = product['body']['amount_of_sold']
            batch_receipt_date = product['body']['batch_receipt_date']
            year, month, day = [int(x) for x in batch_receipt_date.split('-')]
            if products_per_month.get(month) is None:
                products_per_month[month] = (1, [amount_of_sold])
            else:
                count, amount = products_per_month[month]
                products_per_month[month] = (count + 1, amount + [amount_of_sold])
    for month, amount in products_per_month.items():
        print(f'{month}: {amount}')


if __name__ == '__main__':
    test()
