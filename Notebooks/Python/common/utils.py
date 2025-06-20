from delta.tables import DeltaTable
import json
import logging
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, explode_outer
from pyspark.sql.types import *
from typing import List, Optional

class Utils:
    logger = logging.getLogger("Utils")
    logger.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    if not logger.hasHandlers():
        logger.addHandler(stream_handler)

    @staticmethod
    def get_job_url() -> str:
        """
        Extracts the job URL from the notebook run

        Returns:
            str: The URL of the job.
        """
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        notebook_details_nb = json.loads(context.safeToJson())['attributes']
        
        workspace_url = spark.conf.get('spark.databricks.workspaceUrl')
        job_id = notebook_details_nb.get("jobId")
        run_id = notebook_details_nb.get("currentRunId")
        workspace_id = notebook_details_nb.get("orgId")

        if not run_id:
            Utils.logger.info('Executed from interactive cluster.')
            return f"""https://{notebook_details_nb["browserHostName"]}/?o={workspace_id}#notebook/{notebook_details_nb["notebook_id"]}"""
        else:
            Utils.logger.info('Executed from job cluster.')
            return f"https://{workspace_url}/jobs/{job_id}/runs/{run_id}?o={workspace_id}"
    
    @staticmethod
    def dict_to_df(data: dict) -> DataFrame:
        """
        Converts a dictionary into a Spark DataFrame.

        Args:
            data (dict): The dictionary containing the data.
            spark_session (SparkSession): The SparkSession object.

        Returns:
            DataFrame: A Spark DataFrame containing the data from the dictionary.
        """
        pandas_df = pd.DataFrame(data)
        return spark.createDataFrame(pandas_df)

    @staticmethod
    def flatten_dataframe(df: DataFrame, 
                          keep_parent_field_name_flag: bool = False, exclude_array_flatten_cols: str = "") -> DataFrame:
        try:
            fields = df.schema.fields
            field_names = [field.name for field in fields]
            dont_flatten_list = [col.strip() for col in exclude_array_flatten_cols.split(";")]

            for field in fields:
                field_type = field.dataType
                field_name = field.name
                if isinstance(field_type, ArrayType):
                    Utils.logger.info("ArrayType")
                    if field_name not in dont_flatten_list:
                        field_names_excluding_array = [name for name in field_names if name != field_name]
                        field_names_and_explode = field_names_excluding_array + [f"explode_outer({field_name}) as {field_name}"]
                        Utils.logger.info("array => ", field_names_and_explode)
                        exploded_df = df.selectExpr(*field_names_and_explode)
                        
                        return Utils.flatten_dataframe(exploded_df, keep_parent_field_name, exclude_array_flatten_cols)
                elif isinstance(field_type, StructType):
                    Utils.logger.info("StructType")
                    child_field_names = [f"{field_name}.{child_name}" for child_name in field_type.fieldNames()]
                    new_field_names = [name for name in field_names if name != field_name] + child_field_names
                    renamed_cols = [col(name).alias(name.replace(".", "_")) for name in new_field_names]
                    Utils.logger.info("struct => ", renamed_cols)
                    exploded_df = df.select(*renamed_cols)
                    
                    return Utils.flatten_dataframe(exploded_df, keep_parent_field_name, exclude_array_flatten_cols)

            if keep_parent_field_name_flag:
                cols = [col(name).alias(name) for name in field_names]
            else:
                cols = [col(name).alias(name.split("_", 1)[-1]) for name in field_names]
        except Exception as e:
            raise Exception(f"Exception Occurred In Utils.flatten_dataframe() --- {e}")
        return df.select(*cols)

    @staticmethod
    def change_df_column_schema(table_df: DataFrame, column_change_schema: str, source_timestamp_format: str) -> DataFrame:
        updated_df = table_df
        new_cols = column_change_schema.split(";")
        try:
            for col_schema in new_cols:
                col_name, dtype = col_schema.split(":")
                if dtype == "LongType":
                    updated_df = updated_df.withColumn(col_name, updated_df[col_name].cast(LongType()))
                elif dtype == "IntegerType":
                    updated_df = updated_df.withColumn(col_name, updated_df[col_name].cast(IntegerType()))
                elif dtype == "DateType":
                    updated_df = updated_df.withColumn(col_name, updated_df[col_name].cast(DateType()))
                elif dtype == "TimestampType":
                    updated_df = updated_df.withColumn(col_name, updated_df[col_name].cast(TimestampType()))
                elif dtype == "to_timestamp":
                    updated_df = updated_df.withColumn(col_name, to_timestamp(col(col_name), source_timestamp_format))
                elif dtype == "DoubleType":
                    updated_df = updated_df.withColumn(col_name, updated_df[col_name].cast(DoubleType()))
                elif dtype == "BooleanType":
                    updated_df = updated_df.withColumn(col_name, updated_df[col_name].cast(BooleanType()))
                elif dtype == "StringType":
                    updated_df = updated_df.withColumn(col_name, updated_df[col_name].cast(StringType()))
            Utils.logger.info("change_df_column_schema() completed successfully.")
            updated_df.printSchema()
        except Exception as e:
            raise Exception(f"""Exception Occurred in Utils.change_df_column_schema(). Error {e}""")
        return updated_df
    
    @staticmethod
    def rename_columns(table_df: DataFrame, columns_to_rename: str) -> DataFrame:
        updated_df = table_df
        renamed_cols = columns_to_rename.split(";")
        try:
            for col_schema in renamed_cols:
                col_name, new_col_name = col_schema.split(":")
                updated_df = updated_df.withColumnRenamed(col_name, new_col_name)
            Utils.logger.info("rename_columns() completed successfully.")
        except Exception as e:
            raise Exception(f"""Exception Occurred in Utils.rename_columns(). Error {e}""")
        return updated_df

    @staticmethod
    def clean_df_column(data_frame: DataFrame) -> DataFrame:
        clean_df = data_frame
        for col_name in data_frame.columns:
            clean_col_name = col_name.replace(" ", "_").replace("-", "_").replace("/", "_").replace(",", "_").replace("(", "").replace(")", "").replace(":", "").replace("&", "").replace("=", "").replace("*", "").replace(">", "")
            clean_df = clean_df.withColumnRenamed(col_name, clean_col_name)
        return clean_df

    @staticmethod
    def add_columns(source_file_df: DataFrame, additional_columns_in_target: str) -> DataFrame:
        clean_df = source_file_df
        try:
            if "," in additional_columns_in_target:
                columns_array = additional_columns_in_target.split(",")
                for col_name in columns_array:
                    if col_name == "load_ts":
                        clean_df = clean_df.withColumn("load_ts", current_timestamp())
                    else:
                        clean_df = clean_df.withColumn(col_name, lit(""))
            else:
                clean_df = clean_df.withColumn(additional_columns_in_target, current_timestamp())
            Utils.logger.info("add_columns() completed successfully.")
        except Exception as e:
            raise Exception("Exception Occurred In Utils.add_columns(). Error: {e}")
        return clean_df
    
    @staticmethod
    def table_exists(catalogName: str, databaseName: str, tableName: str) -> bool:
        """
        Checks if tableName exists

        Args:
            catalogName: catalog that contains the table
            databaseName: database that contains the table
            tableName: table to check for
            
        Returns:
            True if table exists, False otherwise
        """
        return spark.sql(f"SHOW TABLES IN {databaseName}").filter(f"tableName = '{tableName}'").count() > 0
    
    @staticmethod
    def upsert_delta(
        df: DataFrame, 
        targetCatalog: str,
        targetDatabase: str, 
        targetTableName: str, 
        upsertKeys: List[str],
        upsertDeleteCondition: Optional[str]=None,
        upsertUpdateDict: Optional[dict]=None,
        partitionBy: Optional[List]=None
    ) -> None:
        """
        Perform an upsert operation on a Delta table.

        Args:
            df : The DataFrame to be upserted into the Delta table.
            targetCatalog : The target catalog where the table is located.
            targetDatabase : The target database where the Delta table is located.
            targetTableName : The target Delta table name.
            upsertKeys : List of column names used as keys for upsert operation.
            upsertDeleteCondition : spark SQL syntax condition clause for deleting unmatched records.
            upsertUpdateDict : dictionary of {condition: optional spark SQL condition clause, set: dict {column_name: spark sql value}}
                e.g. f"{{"condition":"`date` >= '2024-01-01'","set": {{"is_active":"False","inactive_date": "'{datetime.datetime.now().strftime('%Y-%m-%d')}'"}}}}" 
            partitionBy : List of column names which is used for partitioning.
            
        Returns:
            None
        """
        Utils.logger.info("Performing Upsert ....")
        mergeCondition = " AND ".join([f"t.{k} = s.{k}" for k in upsertKeys])
        tablePath = targetCatalog + "." + targetDatabase + "." + targetTableName
        dtbl = DeltaTable.forName(spark, tableOrViewName=tablePath)

        if partitionBy is not None:
            #creates in-memory partitions based on input list of col names
            # should match with storage partitions
            df = df.repartition(*partitionBy)

        mergeBuilder = (dtbl.alias("t")
                            .merge(df.alias("s"), mergeCondition)
                            .whenMatchedUpdateAll()
                            .whenNotMatchedInsertAll())    
        
        if upsertDeleteCondition is not None:
            # delete unmatched records given condition
            #   (if you want to delete ALL unmatched records and keep only new records, use Overwrite SNP instead) 
            Utils.logger.info("Deleting unmatched records")
            mergeBuilder = mergeBuilder.whenNotMatchedBySourceDelete(condition=upsertDeleteCondition)
        elif upsertUpdateDict is not None:
            Utils.logger.info("Updating unmatched records")
            mergeBuilder = mergeBuilder.whenNotMatchedBySourceUpdate(condition=upsertUpdateDict.get("condition", None), set=upsertUpdateDict.get("set"))
        mergeBuilder.execute()
        Utils.logger.info("Upsert Done")


    @staticmethod
    def write_data(
        df: DataFrame,
        target_catalog: str,
        target_database: str,
        target_tbl_name: str,
        load_type: str,
        upsert_keys: Optional[List]=None,
        upsert_delete_condition: Optional[str]=None,
        upsert_update_dict: Optional[dict]=None,
        partition_by: Optional[List]=None
    ) -> None:
        """
        Write data to a target table, supporting snapshot load and upsert operations.

        Args:
            df : The DataFrame to be written to the target table.
            target_catalog : The target catalog where the table is located.
            target_database : The target database where the table is located.
            target_tbl_name : The target table name.
            load_type: The type to write table to database.
            upsert_keys : List of column names used as keys for upsert operation.
            upsert_delete_condition: spark SQL syntax condition clause for deleting unmatched records.
            partition_by : The column name which is used as partition flag.
            
        Returns:
            None
        """

        Utils.logger.info("Target Catalog Name: " + target_catalog)
        Utils.logger.info("Target Database: " + target_database)
        Utils.logger.info("Target Table Name: " + target_tbl_name)

        tableExists = Utils.table_exists(target_catalog, target_database, target_tbl_name)

        try:
            if load_type == "SNP" or not tableExists:
                tableUtils.saveAsTable(
                    df,
                    full_target_table_name=f"{target_catalog}.{target_database}.{target_tbl_name}",
                    write_mode="overwrite",
                    partition=partition_by,
                    debug=False,
                    dry_run=False,
                )

            elif load_type == "INC":
                Utils.upsert_delta(df, 
                            target_catalog, 
                            target_database, 
                            target_tbl_name, 
                            upsertKeys=upsert_keys, 
                            upsertDeleteCondition=upsert_delete_condition,
                            upsertUpdateDict=upsert_update_dict, 
                            partitionBy=partition_by)

            else:
                raise Exception("Incorrect load_type Given")
        except Exception as e:
            raise Exception(f"Error writing data to table: {target_tbl_name}. Error: {e}")


