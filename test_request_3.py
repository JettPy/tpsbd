import json


def test():
    customers = {}
    with open('purchases.json') as file:
        purchases = json.load(file)
        for purchase in purchases:
            customer_id = purchase['body']['customer_id']
            customer = purchase['body']['personal_data']
            price = purchase['body']['price']
            if customers.get(customer_id) is None:
                customers[customer_id] = (customer, price)
            else:
                customers[customer_id] = (customer, price + customers[customer_id][1])
    customer = ''
    max_total_price = 0
    for name, summa in customers.values():
        if summa > max_total_price:
            customer = name
            max_total_price = summa
    print(f'Покупатель с большей суммой: {customer} - {max_total_price}')


if __name__ == '__main__':
    test()