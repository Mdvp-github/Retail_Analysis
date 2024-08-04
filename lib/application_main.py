import sys
import DataManipulation, Utils, DataReader
from pyspark.sql.functions import * 
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
    print(sys.argv)
    
    
    job_run_env = "LOCAL"
    

    
    print("Creating Spark Session") 
    spark = Utils.get_spark_session(job_run_env) 
    
    print("Created Spark Session") 

    orders_df = DataReader.read_orders(spark,job_run_env) 
    orders_filtered = DataManipulation.filter_closed_orders(orders_df) 
    customers_df = DataReader.read_customers(spark,job_run_env)
    joined_df =DataManipulation.join_orders_customers(orders_df,customers_df)
    aggregated_results = DataManipulation.count_orders_state(joined_df) 
    aggregated_results.show() 
print("end of main")
