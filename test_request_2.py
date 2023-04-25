from datetime import datetime
import json


def test():
    price_for_2_months = 0
    total_price = 0
    with open('purchases.json') as file:
        purchases = json.load(file)
        for purchase in purchases:
            year, month, day = [int(x) for x in purchase['body']['purchase_date'].split('-')]
            purchase_date = datetime(year, month, day)
            now = datetime.now()
            last_month = now.month - 2 if now.month > 2 else now.month - 2 + 12
            last_year = now.year if last_month < now.month else now.year - 1
            last_date = datetime(last_year, last_month, 1)
            if last_date < purchase_date:
                price_for_2_months += purchase['body']['price']
            total_price += purchase['body']['price']
    print(f'За 2 месяца: {price_for_2_months}, Всего: {total_price}')


if __name__ == '__main__':
    test()
