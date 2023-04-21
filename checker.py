import json
shop = {}
with open("purchases.json", "r", encoding="UTF-8") as file:
    purchases = json.load(file)
    for purchase in purchases:
        product_id = purchase['body']['product_id']
        product = purchase['body']['product_name']
        amount = purchase['body']['amount']
        total_cost = purchase['body']['price']
        if shop.get(product_id) is None:
            shop[product_id] = [product, amount, total_cost]
        else:
            shop[product_id] = [product, amount + shop[product_id][1], total_cost + shop[product_id][2]]

with open("products.json", "r", encoding="UTF-8") as file:
    products = json.load(file)
    for product in products:
        product_id = product['id']
        product_name = product['body']['product_name']
        amount_of_sold = product['body']['amount_of_sold']
        price = product['body']['price']
        # print(product_id, product_name, amount_of_sold, amount_of_sold * price)
        if shop.get(product_id) is None:
            if (amount_of_sold != 0):
                print("Error:", product_id, product_name, amount_of_sold, price)
            else:
                shop[product_id] = [product_name, amount_of_sold, amount_of_sold * price]
        else:
            tmp_list = [product_name, amount_of_sold - shop[product_id][1], shop[product_id][2] - amount_of_sold * price]
            shop[product_id] = tmp_list.copy()

for id, product in shop.items():
    print(f'{id}: {product}')
