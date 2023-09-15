import random
import json

import datetime
import time
from kashpy.kash import *

c = Cluster("local")
id = 0
random.seed(42)
while True:
    row = json.dumps({
        'id': id,
        'from_account': random.randint(0,9),
        'to_account': random.randint(0,9),
        'amount': 1,
        'ts': datetime.datetime.now().isoformat()
    })
    print(row)
    c.produce("transactions", row, key=str(id))
    if id % 1000 == 0:
      c.flush()
    id += 1
    time.sleep(1)
