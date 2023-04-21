from datetime import datetime
import json


price_for_2_months = 0
total_price = 0
with open("purchases.json", "r", encoding="UTF-8") as file:
    purchases = json.load(file)
    for purchase in purchases:
        year, month, day = [int(x) for x in purchase['body']['purchase_date'].split('-')]
        purchase_date = datetime(year, month, day)
        days = (datetime.now() - purchase_date).days
        if days < 80:
            price_for_2_months += purchase['body']['price']
        total_price += purchase['body']['price']

print(price_for_2_months, total_price)
