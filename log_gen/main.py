#
# LogVision Log Generator
#
# For simulation of log appending, implemented in a non-chronological way.
#
# Xander Wang, 1st, Nov, 2018
#

import random
import linecache
import time

stock_log_path = "../log_repo/stock.log"
target_log_path = "../log_repo/src.log"

stock_log = linecache.getlines(stock_log_path)

sleep_time = 1
maximum_items = 10

while True:
    target_log = open(target_log_path, 'a+')
    start = random.randint(1, len(stock_log))
    end = start + random.randint(1, maximum_items)
    if end > len(stock_log):
        start = random.randint(1, len(stock_log))
        end = start + random.randint(1, maximum_items)
    clip = stock_log[start:end]
    for i in clip:
        target_log.write(i)
    print("[LogVision] Appended " + str(len(clip)) + " record(s).")
    target_log.close()
    time.sleep(sleep_time)
