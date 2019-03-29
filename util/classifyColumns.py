from libraries.libraries import *

def get_columns(coltype,all_col_types):
	timestamp_columns_types = filter(lambda x:x[1]==coltype,all_col_types)
	return map(lambda x:x[0],timestamp_columns_types)
