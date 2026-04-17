import os
from clickhouse_connect import get_client
from dotenv import load_dotenv

load_dotenv()

client = get_client(
    host=os.environ.get('CH_HOST'), 
    port=int(os.environ.get('CH_PORT')), 
    username=os.environ.get('CH_USER'), 
    password=os.environ.get('CH_PASSWORD'), 
    database=os.environ.get('CH_DATABASE')
)

# Test 1: Count subfield in works
res = client.query("SELECT count() FROM works WHERE subfield = 'Pulmonary and Respiratory Medicine'")
print(f"Count in works: {res.result_rows}")

# Test 2: Check any row in summing_subfield_metrics
res = client.query("SELECT count() FROM summing_subfield_metrics")
print(f"Count in summing_subfield_metrics: {res.result_rows}")
