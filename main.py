import test_request_1
import test_request_2
import test_request_3
import test_request_4
from checker import check_data
from data_generator import generate
from init_es_db import init_es_db
from init_neo_db import init_neo_db

print('=' * 20)
generate()
print('=' * 20)
check_data()
print('=' * 20)
init_es_db()
print('=' * 20)
init_neo_db()
print('=' * 20)
test_request_1.test()
print('=' * 20)
test_request_2.test()
print('=' * 20)
test_request_3.test()
print('=' * 20)
test_request_4.test()