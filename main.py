import nav_calc
import nav_test
from datetime import datetime, timezone
import time

start = time.time()
print("starting at：", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"))
# nav_calc.main()

nav_test.main()
end = time.time()

print('time taken: ', end-start)
# print('new nav calc logic')
# print("completed")
