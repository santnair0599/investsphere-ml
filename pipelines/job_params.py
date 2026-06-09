"""
Read a parameter value safely.

This function works in 3 situations:

1. Databricks job / widget parameter
2. Command-line argument like: --run_date 2026-05-29
3. Default value for local testing

Example:
    RUN_DATE = get_param("run_date", "2026-05-29")
"""

import sys

def get_param(parameter_name, default_value):
    """
    Get a parameter value.

    Priority:
    1. First try Databricks widget/job parameter.
    2. Then try command-line argument.
    3. If nothing is found, return default value.
    """

    # --------------------------------------------------
    # 1. Try to read from Databricks job/widget parameter
    # --------------------------
    try:
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils
        
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        
        databricks_value = dbutils.widgets.get(parameter_name)
        
        if databricks_value:
            return databricks_value
        
    except Exception:
        # If this code is not running in Databricks,
        # or if the widget does not exist, ignore the error.
        pass
    
     # --------------------------------------------------
    # 2. Try to read from command-line arguments
    # --------------------------------------------------
    # Example command:
    # python script.py --run_date 2026-05-29
    #
    # sys.argv will look like:
    # ["script.py", "--run_date", "2026-05-29"]
    
    command_line_flag = "--" + parameter_name
    
    if command_line_flag in sys.argv:
        flag_position = sys.argv.index(command_line_flag)
        
        value_position = flag_position + 1
        
        if value_position < len(sys.argv):
            command_line_value = sys.argv[value_position]
            return command_line_value
    
    #---------------------------------------------
    # 3. Return default value
    #-------------------------------------------------------
    return default_value