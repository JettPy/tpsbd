import test_request_1
import test_request_2
import test_request_3
from checker import check_data
from data_generator import generate
from init_es_db import init_es_db
from init_neo_db import init_neo_db

generate()
check_data()
init_es_db()
init_neo_db()
test_request_1.test()
test_request_2.test()
test_request_3.test()