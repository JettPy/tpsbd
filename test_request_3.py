import json

customers = {}
with open("purchases.json", "r", encoding="UTF-8") as file:
    purchases = json.load(file)
    for purchase in purchases:
        customer_id = purchase['body']['customer_id']
        customer = purchase['body']['personal_data']
        price = purchase['body']['price']
        if customers.get(customer_id) is None:
            customers[customer_id] = (customer, price)
        else:
            customers[customer_id] = (customer, price + customers[customer_id][1])

customer_id = -1
customer = ''
max_total_price = 0
for c_id, data in customers.items():
    if data[1] > max_total_price:
        customer = data[0]
        customer_id = c_id
        max_total_price = data[1]
print(customer, max_total_price)
