import json
import random
from datetime import datetime
import time
from constants import CUSTOMERS_N, PURCHASES_N, PRODUCTS_N, CUSTOMERS, PRODUCTS


def date_generator():
    curr_time_ns = time.time_ns()
    bordered_time_ns = int(curr_time_ns - 1.6 * 1e16)
    time_ns = random.randint(bordered_time_ns, curr_time_ns)
    date = datetime.fromtimestamp(time_ns / 1e9)
    return date.year, date.month, date.day


def is_next_date(date_first: tuple, date_last: tuple):
    if date_first[0] != date_last[0]:
        return date_first[0] < date_last[0]
    elif date_first[1] != date_last[1]:
        return date_first[1] < date_last[1]
    else:
        return date_first[2] < date_last[2]


def get_second_date(date: tuple):
    year_next, month_next, day_next = date
    while not is_next_date(date, (year_next, month_next, day_next)):
        year_next, month_next, day_next = date_generator()
    return year_next, month_next, day_next


def translate(word: str):
    letters = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'j', 'з': 'z', 'и': 'i', 'й': 'i',
        'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f',
        'х': 'h', 'ц': 'c', 'ч': 'ch', 'ш': 'sh', 'щ': 'sh', 'ъ': '', 'ы': 'i', 'ь': '', 'э': 'a', 'ю': 'yu', 'я': 'ya'
    }
    translated_word = ''
    for char in word.lower():
        translated_word += letters[char]
    return translated_word


def generate():
    customers_count = CUSTOMERS_N if CUSTOMERS_N <= len(CUSTOMERS) else len(CUSTOMERS)
    products_count = PRODUCTS_N if PRODUCTS_N <= len(PRODUCTS) else len(PRODUCTS)
    purchases_count = PURCHASES_N

    products_list = []
    purchases_list = []
    is_used = set()
    for i in range(products_count):
        product = {'index': 'product', 'doc_type': 'product', 'id': i}
        body = {}
        product_id = random.randint(0, len(PRODUCTS) - 1)
        product_info = PRODUCTS[product_id]
        retry_count = len(PRODUCTS)
        while product_id in is_used and retry_count > 0:
            product_id = (product_id + 1) % len(PRODUCTS)
            product_info = PRODUCTS[product_id]
            retry_count -= 1
        if retry_count == 0:
            break
        is_used.add(product_id)
        body['product_name'] = product_info['name']
        year, month, day = date_generator()
        body['batch_receipt_date'] = '%04d-%02d-%02d' % (year, month, day)
        amount_first, amount_last = product_info['amount']
        body['amount_in_stock'] = random.randint(amount_first, amount_last)
        body['amount_of_sold'] = 0
        body['price'] = product_info['price']
        body['description'] = product_info['description']
        link = translate(product_info['name'])
        body['image_link'] = f'images/{link}.jpg'
        product['body'] = body
        products_list.append(product)

    for i in range(purchases_count):
        purchase = {'index': 'purchase', 'doc_type': 'purchase', 'id': i}
        body = {}
        customer_id = random.randint(0, customers_count - 1)
        body['customer_id'] = customer_id
        body['personal_data'] = CUSTOMERS[customer_id]
        product_id = random.randint(0, products_count - 1)
        product = products_list[product_id]
        retry_count = products_count
        while product['body']['amount_in_stock'] == 0 and retry_count > 0:
            product_id = (product_id + 1) % products_count
            product = products_list[product_id]
            retry_count -= 1
        if retry_count == 0:
            break
        year_batch, month_batch, day_batch = [int(x) for x in product['body']['batch_receipt_date'].split('-')]
        year, month, day = get_second_date((year_batch, month_batch, day_batch))
        body['purchase_date'] = '%04d-%02d-%02d' % (year, month, day)
        body['product_id'] = product['id']
        body['product_name'] = product['body']['product_name']
        body['amount'] = random.randint(1, product['body']['amount_in_stock'])
        products_list[product_id]['body']['amount_of_sold'] += body['amount']
        products_list[product_id]['body']['amount_in_stock'] -= body['amount']
        body['price'] = product['body']['price'] * body['amount']
        purchase['body'] = body
        purchases_list.append(purchase)

    with open('purchases.json', 'w') as file:
        json.dump(purchases_list, file, indent=4)

    with open('products.json', 'w') as file:
        json.dump(products_list, file, indent=4)


if __name__ == '__main__':
    generate()