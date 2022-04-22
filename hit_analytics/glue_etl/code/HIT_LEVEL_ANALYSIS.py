import sys
import re
from hitanalysis import hitanalysis
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, explode, split, col, regexp_extract, desc,lower,lit
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_file_to_process'])
print(args)

# Defining Spark env 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# Instantiating the hit object
hit = hitanalysis()
# Registering extract_product as UDF
extract_product = udf(hit.extract_info, ArrayType(ArrayType(StringType())))

# Registering parse_ref as UDF
parse_ref = udf(hit.parse_reff, StringType())




data_file = args['input_file_to_process']

hit_level_df = spark.read.csv(data_file, sep=r'\t', header=True)

user_and_ref = hit_level_df.select('ip', col('referrer').alias('ref_u'))\
                           .where(hit_level_df.referrer.like('%google%') | \
                                   hit_level_df.referrer.like('%yahoo%') |\
                                   hit_level_df.referrer.like('%bing%')) 

hit_level_ref = hit_level_df.join(user_and_ref, on=['ip'], how='left') 


hit_level_domain = hit_level_ref.filter(hit_level_ref.event_list == 1)\
                                .withColumn('search_query', parse_ref('ref_u')) \
                                .withColumn('search_domain', \
                             regexp_extract('ref_u', r'^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)', 1)\
                                .alias('domain'))
                             
                             
hit_level_exp = hit_level_domain.select(hit_level_domain['*'], \
                                    explode(extract_product(hit_level_df.product_list)))

#column_names = ["category", "product_name", "Number_of_items", "total_revenue","custom_event","Merchandizing_eVar" ]
#for index, column in enumerate(column_names):
#  hit_level_exp_product_df = hit_level_exp.withColumn(column, col('col').getItem(index))


hit_level_exp_rev   = hit_level_exp.select(hit_level_exp['*'], \
                                 hit_level_exp.col[0].alias('category'), \
                                 hit_level_exp.col[1].alias('product_name'),\
                                 hit_level_exp.col[2].cast('int').alias('Number_of_items'),\
                                 hit_level_exp.col[3].cast('int').alias('total_revenue'),\
                                 hit_level_exp.col[4].cast('int').alias('custom_event'),\
                                 hit_level_exp.col[5].alias('Merchandizing_eVar'))\
                                 .withColumn('total_rev', col('Number_of_items') * col('total_revenue'))



hit_level_exp_rev_per_domain = hit_level_exp_rev.groupBy("search_domain",lower(hit_level_exp_rev.search_query)\
                                                .alias('search_query'))\
                                                .sum('total_rev').alias('total_rev')





hit_level_exp_rev_per_domain.write.option("header",True) \
                            .option("sep", r'\t') \
                            .option("encoding", "UTF-8")\
                            .csv("s3://pk-deltalake-bucket/out/")



job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()