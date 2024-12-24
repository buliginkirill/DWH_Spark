from contextlib import contextmanager
from datetime import datetime

from pyspark.sql import SparkSession, conf
from pyspark.sql.functions import col, max as spark_max, lit, avg, count, min as spark_min, median, when, sequence,\
    date_trunc, expr, explode, last_day, datediff, first, sum as spark_sum, count_distinct, current_timestamp, year,\
    date_format
from pyspark.sql.window import Window

from src.log import Logger
from src.settings import SPARK_HOST, SPARK_PORT, SPARK_DRIVER_IP, PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
import os

#os.environ['SPARK_LOCAL_IP'] = SPARK_DRIVER_IP

class Processor:
    jdbc_driver_path = "./jdbc/postgresql-42.7.4.jar"
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    properties = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    def __init__(self):
        self.load_dttm = None

    @contextmanager
    def get_spark(self):
        # .config("spark.driver.bindAddress", SPARK_DRIVER_IP) \
        spark = SparkSession.builder \
            .appName("MySparkApp") \
            .master(f"spark://{SPARK_HOST}:{SPARK_PORT}") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.driver.cores", "2") \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.driver.extraClassPath", self.jdbc_driver_path) \
            .config("spark.executor.extraClassPath", self.jdbc_driver_path) \
            .getOrCreate()
        yield spark
        spark.stop()

    def generate_load_dttm(self, spark):
        self.load_dttm = datetime.now()
        df = spark.read.jdbc(url=self.jdbc_url,
                             table='(select * from dwh.load_dates_craftsman_report_datamart) as tmp',
                             properties=self.properties)
        max_id = df.agg(spark_max(col("id"))).collect()[0][0]

        # Show the record with the maximum 'id' value
        Logger.log("Record with maximum 'id' value:")
        append_df = spark.createDataFrame(
            [((max_id or 0)+1, self.load_dttm,)],
            schema=df.schema
        )
        append_df.write.mode('append').jdbc(url=self.jdbc_url, table='dwh.load_dates_craftsman_report_datamart', properties=self.properties)

    def load_dictionary(self, spark, columns, key_column, target_table_name, source_table_name):

        if not self.load_dttm:
            raise Exception("No load_dttm generated")

        source_df = spark.read.jdbc(url=self.jdbc_url,
                                     table=f'(select * from {source_table_name}) as tmp',
                                     properties=self.properties)
        target_df = spark.read.jdbc(url=self.jdbc_url,
                                     table=f'(select {key_column}, load_dttm from {target_table_name}) as tmp',
                                     properties=self.properties)

        source_df = source_df.select(columns).distinct()
        existing_ids_df = target_df.select(key_column)
        incremental_df = source_df.join(existing_ids_df, on=key_column, how="left_anti")
        incremental_df = incremental_df.withColumn("load_dttm", lit(self.load_dttm))
        incremental_df = incremental_df.select([*columns, "load_dttm"])
        incremental_df.write.jdbc(url=self.jdbc_url,
                                  table=target_table_name,
                                  mode='append',
                                  properties=self.properties)

    def load_d_craftsmans(self, spark):
        Logger.log("Loading d_craftsmans...")
        columns = ["craftsman_id", "craftsman_name","craftsman_address","craftsman_birthday","craftsman_email"]
        key_column = "craftsman_id"
        source_table_name = "source3.craft_market_craftsmans"
        target_table_name = "dwh.d_craftsmans"
        self.load_dictionary(spark, columns, key_column, target_table_name, source_table_name)

    def load_d_customers(self, spark):
        Logger.log("Loading d_customers...")
        columns = ["customer_id", "customer_name","customer_address","customer_birthday","customer_email"]
        key_column = "customer_id"
        source_table_name = "source3.craft_market_customers"
        target_table_name = "dwh.d_customers"
        self.load_dictionary(spark, columns, key_column, target_table_name, source_table_name)

    def load_d_products(self, spark):
        Logger.log("Loading d_products...")
        columns = ["product_id", "product_name","product_description","product_type","product_price"]
        key_column = "product_id"
        source_table_name = "source2.craft_market_masters_products"
        target_table_name = "dwh.d_products"
        self.load_dictionary(spark, columns, key_column, target_table_name, source_table_name)

    def load_f_orders(self, spark):
        Logger.log("Loading f_orders...")
        columns = ["order_id", "product_id", "craftsman_id", "customer_id", "order_created_date", "order_completion_date", "order_status"]
        key_column = "order_id"
        source_table_name = "source2.craft_market_orders_customers"
        target_table_name = "dwh.f_orders"
        self.load_dictionary(spark, columns, key_column, target_table_name, source_table_name)

    def build_craftsman_report_datamart(self, spark):
        Logger.log("Building craftsman_report_datamart...")

        # Загружаем таблицы справочников и фактов
        craftsmans_df = spark.read.jdbc(url=self.jdbc_url,
                                        table='dwh.d_craftsmans',
                                        properties=self.properties)
        customers_df = spark.read.jdbc(url=self.jdbc_url,
                                       table='dwh.d_customers',
                                       properties=self.properties)
        products_df = spark.read.jdbc(url=self.jdbc_url,
                                      table='dwh.d_products',
                                      properties=self.properties)
        orders_df = spark.read.jdbc(url=self.jdbc_url,
                                    table='dwh.f_orders',
                                    properties=self.properties)

        orders_df = orders_df.withColumn('order_created_month',
                    date_trunc('month', 'order_created_date')
                )
        #----------------------------------------------------------------------
        # craftsman_money     -- сумма денег, которую заработал мастер (-10% на платформы) за месяц
        # platform_money      -- сумма денег, которая заработала платформа от продаж мастера за месяц
        result_1 = orders_df\
            .join(products_df,on="product_id")\
            .groupBy('craftsman_id','order_created_month').agg(
                spark_sum(col("product_price") * 0.9).alias("craftsman_money"),
                spark_sum(col("product_price") * 0.1).alias("platform_money"),
        )
        #----------------------------------------------------------------------
        # count_order         -- количество заказов у мастера за месяц
        # count_order_created -- количество созданных заказов за месяц
        # count_order_in_progress -- количество заказов в процессе изготовки за месяц
        # count_order_delivery -- количество заказов в доставке за месяц
        # count_order_done    -- количество завершенных заказов за месяц
        # count_order_not_done -- количество незавершенных заказов за месяц
        # median_time_order_completed -- медианное время в днях от момента создания заказа до его завершения  за месяц
        result_2 = orders_df\
            .groupBy('craftsman_id','order_created_month').agg(
                count_distinct(col('order_id')).alias('count_order'),
                count_distinct(when(col("order_status") == "created", 'order_id')).alias("count_order_created"),
                count_distinct(when(col("order_status") == "in_progress", 'order_id')).alias("count_order_in_progress"),
                count_distinct(when(col("order_status") == "delivery", 'order_id')).alias("count_order_delivery"),
                count_distinct(when(col("order_status") == "done", 'order_id')).alias("count_order_done"),
                count_distinct(when(col("order_status") == "not_done", 'order_id')).alias("count_order_not_done"),
                avg(when(col("order_status") == "done",datediff(col("order_completion_date"), col("order_created_date")))).alias("median_time_order_completed"),
        )
        # ----------------------------------------------------------------------
        # avg_price_order -- средняя стоимость одного заказа у мастера за месяц
        result_3 = orders_df\
            .join(products_df,on="product_id")\
            .groupBy('craftsman_id','order_created_month','order_id')\
            .agg(spark_sum(col('product_price')).alias('order_amount'))\
            .groupBy('craftsman_id','order_created_month')\
            .agg(avg(col('order_amount')).alias('avg_price_order'))
        # ----------------------------------------------------------------------
        # avg_age_customer    -- средний возраст покупателей
        result_4 = orders_df\
        .join(customers_df, on="customer_id") \
        .groupBy(['craftsman_id', 'order_created_month']) \
        .agg(avg(datediff(current_timestamp(), col('customer_birthday'))/ 365.25).alias('avg_age_customer'))
        # ----------------------------------------------------------------------
        # top_product_category -- самая популярная категория товаров у этого мастера  за месяц
        result_5 = orders_df\
        .join(products_df, on='product_id')\
        .groupBy(['craftsman_id', 'order_created_month','product_type'])\
        .agg(count(col('product_id')).alias('category_count'))\
        .withColumn('max_category_count', spark_max(col('category_count')).over(
            Window.partitionBy(['craftsman_id', 'order_created_month'])
        ))\
        .filter("category_count=max_category_count")\
        .select(col('craftsman_id'),col('order_created_month'),col('product_type').alias('top_product_category'))
        # ----------------------------------------------------------------------
        # Итоговый результат
        join_columns = ['craftsman_id', 'order_created_month']
        result_df = craftsmans_df\
        .join(orders_df.select(['craftsman_id', 'order_created_month']), on='craftsman_id')\
        .join(result_1, on=join_columns, how='left')\
        .join(result_2, on=join_columns, how='left')\
        .join(result_3, on=join_columns, how='left')\
        .join(result_4, on=join_columns, how='left')\
        .join(result_5, on=join_columns, how='left')\
        .withColumn('report_period', date_format(col("order_created_month"), "yyyy-MM"))

        result_df = result_df\
        .select(
            col('craftsman_id'),
            col('craftsman_name'),      # ФИО мастера
            col('craftsman_address'),   # адрес мастера
            col('craftsman_birthday'),  # дата рождения мастера
            col('craftsman_email'),     # электронная почта мастера
            col('craftsman_money'),     # сумма денег, которую заработал мастер (-10% на платформы) за месяц
            col('platform_money'),      # сумма денег, которая заработала платформа от продаж мастера за месяц
            col('count_order'),         # количество заказов у мастера за месяц
            col('avg_price_order'),     # средняя стоимость одного заказа у мастера за месяц
            col('avg_age_customer'),    # средний возраст покупателей
            col('median_time_order_completed'), # медианное время в днях от момента создания заказа до его завершения  за месяц
            col('top_product_category'), # самая популярная категория товаров у этого мастера  за месяц
            col('count_order_created'), # количество созданных заказов за месяц
            col('count_order_in_progress'), # количество заказов в процессе изготовки за месяц
            col('count_order_delivery'), # количество заказов в доставке за месяц
            col('count_order_done'),    # количество завершенных заказов за месяц
            col('count_order_not_done'), # количество незавершенных заказов за месяц
            col('report_period')       # отчетный период год и месяц
        )

        # Write the results to the craftsman_report_datamart table
        result_df.write.jdbc(url=self.jdbc_url,
                             table='dwh.craftsman_report_datamart',
                             mode='overwrite',
                             properties=self.properties)

    def load_all_data(self):
        Logger.log('Loading dwh data...')
        with self.get_spark() as spark:
            self.generate_load_dttm(spark)
            self.load_d_craftsmans(spark)
            self.load_d_customers(spark)
            self.load_d_products(spark)
            self.load_f_orders(spark)
            self.build_craftsman_report_datamart(spark)
        Logger.log('Done.')

    def erase_all_data(self):
        Logger.log("Cleaning all dwh tables!")
        with self.get_spark() as spark:
            tables = [
                'dwh.craftsman_report_datamart',
                'dwh.f_orders',
                'dwh.d_craftsmans',
                'dwh.d_customers',
                'dwh.d_products',
                'dwh.load_dates_craftsman_report_datamart',
            ]
            for s in tables:
                df = spark.read.jdbc(url=self.jdbc_url, table=s, properties=self.properties)
                df.filter('1=0').write\
                    .mode('overwrite')\
                    .jdbc(url=self.jdbc_url, table=s, properties=self.properties)
        Logger.log('Done.')
