import neo_request
from data_generator import generate
import es_request_1
import es_request_2
from init_es_db import init_es_db
from init_neo_db import init_neo_db

print('=' * 20)
generate()
print('=' * 20)
init_es_db()
print('=' * 20)
es_request_1.request()
print('=' * 20)
es_request_2.request()
print('=' * 20)
init_neo_db()
print('=' * 20)
neo_request.request()