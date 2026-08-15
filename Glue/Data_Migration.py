import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Script generated for node Total_value
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # Importing Functions
    from pyspark.sql.functions import col

    # Get the input DynamicFrame
    dyf = dfc.select(list(dfc.keys())[0])

    # Convert to DataFrame
    df = dyf.toDF()

    # Multiply two columns
    df = df.withColumn(
        "total_value",
        col("Price") * col("Quantity")
    )

    # Convert back to DynamicFrame
    result_dyf = DynamicFrame.fromDF(df, glueContext, "result_dyf")

    return DynamicFrameCollection({"result": result_dyf}, glueContext)
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Orders_source
Orders_source_node1786423014927 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://buckettutorialanuj378/raw/"], "recurse": True}, transformation_ctx="Orders_source_node1786423014927")

# Script generated for node Drop Duplicates
DropDuplicates_node1786423494607 =  DynamicFrame.fromDF(Orders_source_node1786423014927.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1786423494607")

# Script generated for node Change Schema
ChangeSchema_node1786423676356 = ApplyMapping.apply(frame=DropDuplicates_node1786423494607, mappings=[("orderid", "string", "orderid", "string"), ("customer", "string", "customer", "string"), ("item", "string", "item", "string"), ("quantity", "string", "quantity", "string"), ("price", "string", "price", "string"), ("orderdate", "string", "orderdate", "string")], transformation_ctx="ChangeSchema_node1786423676356")

# Script generated for node Total_value
Total_value_node1786423608997 = MyTransform(glueContext, DynamicFrameCollection({"ChangeSchema_node1786423676356": ChangeSchema_node1786423676356}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1786424548354 = SelectFromCollection.apply(dfc=Total_value_node1786423608997, key=list(Total_value_node1786423608997.keys())[0], transformation_ctx="SelectFromCollection_node1786424548354")

# Script generated for node SQL Query
SqlQuery938 = '''
SELECT * from myDataSource
WHERE total_value>1000;
'''
SQLQuery_node1786424247926 = sparkSqlQuery(glueContext, query = SqlQuery938, mapping = {"myDataSource":SelectFromCollection_node1786424548354}, transformation_ctx = "SQLQuery_node1786424247926")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1786592056078_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        ColumnCount = 7
       
    ]
"""

EvaluateDataQuality_node1786592056078 = EvaluateDataQuality().process_rows(frame=SQLQuery_node1786424247926, ruleset=EvaluateDataQuality_node1786592056078_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786592056078", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node Aggregate
Aggregate_node1786424961553 = sparkAggregate(glueContext, parentFrame = SQLQuery_node1786424247926, groups = [], aggs = [["total_value", "sum"]], transformation_ctx = "Aggregate_node1786424961553")

# Script generated for node Select From Collection
SelectFromCollection_node1786592712703 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1786592056078, key="Select From Collection", transformation_ctx="SelectFromCollection_node1786592712703")

# Script generated for node Rename Field
RenameField_node1786425500080 = RenameField.apply(frame=Aggregate_node1786424961553, old_name="`sum(total_value)`", new_name="total_value", transformation_ctx="RenameField_node1786425500080")

# Script generated for node Sink S3
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1786592712703, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786422681371", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
SinkS3_node1786424854523 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1786592712703, connection_type="s3", format="glueparquet", connection_options={"path": "s3://buckettutorialanuj378/S3sink/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="SinkS3_node1786424854523")

# Script generated for node S3 Aggregated
additional_options = {"path": "s3://buckettutorialanuj378/S3sinkagg/", "write.parquet.compression-codec": "snappy"}
S3Aggregated_node1786424870513_df = RenameField_node1786425500080.toDF()
S3Aggregated_node1786424870513_df.write.format("delta").options(**additional_options).mode("append").save()

job.commit()