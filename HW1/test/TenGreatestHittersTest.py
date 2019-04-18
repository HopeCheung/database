import sys
sys.path.append(sys.path[0][:-5]+"/src")
from TenGreatestHitters import CSV_TOP, RDB_TOP
print("Test function for CSV")
CSV_TOP()

print("Test function by RDB")
RDB_TOP()
