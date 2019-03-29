from libraries.libraries import *
from util.classifyColumns import get_columns

# get the feature columns
jsnDf  = spark.read.json("file:///home/alineuser/manju/test/config/config.json")
feature_columns = ""
feature_columns = jsnDf.first()[0].split(",")
#print(feature_columns)

#getting target column
target_column = jsnDf.first()[1]
#print(target_column)

#read the input file
inputdf = spark.read.csv("file:///home/alineuser/manju/test/data/HR_comma_sep.csv",inferSchema=True,header=True)
print("count of a file is",inputdf.count())

all_columns_datatype = inputdf.dtypes
#print(all_columns_datatype)

#get the timestamp columns
timestamp_columns = get_columns("timestamp",all_columns_datatype)
print(timestamp_columns)

#get numeric columns
int_columns = get_columns("int",all_columns_datatype)
double_columns = get_columns("double",all_columns_datatype)
numeric_columns = int_columns+double_columns
print(numeric_columns)

#get string columns
string_columns = get_columns("string",all_columns_datatype)
print(string_columns)


final_feature_columns = set(feature_columns) - set(timestamp_columns)
print(final_feature_columns)

#drop the time/date columns if it is not a time series
inputdf1 = inputdf.select([column for column in inputdf.columns if column not in timestamp_columns])
inputdf1.show()


num_columns = []
txt_columns = []
cat_columns = []

for col in final_feature_columns:
	df = inputdf1.groupBy(col).count()
	if(df.count() <= 3):
		cat_columns.append(col)
	else:
		if(col in numeric_columns):
			num_columns.append(col)
		else:
			txt_columns.append(col)

print("num_columns are:{}".format(num_columns))
print("txt columns are:{}".format(txt_columns))
print("cat_columns:{}".format(cat_columns))

#stringIndexer stages
catcol_stringindex_stages = [StringIndexer(inputCol=c,outputCol="strindx_"+c) for c in cat_columns]
txtcol_strindex_stages = [StringIndexer(inputCol=c,outputCol="strindx_"+c) for c in txt_columns]
label_strindex_stage = [StringIndexer(inputCol=target_column, outputCol='label')]

#OHE stages
onehotencoder_stages = [OneHotEncoder(inputCol="strindx_"+c,outputCol="onehot_"+c) for c in cat_columns]

#vector Assembler stages
features_col = ["onehot_"+c for c in cat_columns]+num_columns+["strindx_"+c for c in txt_columns]
print(features_col)
vectorassembler_stage = VectorAssembler(inputCols=features_col, outputCol='features')

pipeline = Pipeline(stages=catcol_stringindex_stages+txtcol_strindex_stages+label_strindex_stage+onehotencoder_stages+[vectorassembler_stage])
pipeline.fit(inputdf1).transform(inputdf1).select('features','label').show(10,False)

