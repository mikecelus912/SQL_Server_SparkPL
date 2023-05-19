import os
import findspark
findspark.init()
from pyspark.sql import SparkSession

#found it best to create a class that would initialize the spark connection while also creating the dataframes to be used for each function
class SparkConnection:
    def __init__(self):
        # Create SparkSession
        self.spark = SparkSession.builder \
            .appName("SQLQueryExecution") \
            .getOrCreate()

        # Load the required Parquet files into DataFrames for menu choice 1
        self.sales_person_df = self.spark.read.parquet("AdventureWorks2019/Sales/SalesPerson.parquet")
        self.sales_territory_df = self.spark.read.parquet("AdventureWorks2019/Sales/SalesTerritory.parquet")
        self.employee_df = self.spark.read.parquet("AdventureWorks2019/HumanResources/Employee.parquet")
        self.person_df = self.spark.read.parquet("AdventureWorks2019/Person/Person.parquet")

        # Load the Parquet file into a DataFrame for menu choice 2
        self.tbl_constraint_df = self.spark.read.parquet("AdventureWorks2019/dbo/tbl_CheckConstraint.parquet")

        #transform dfs into temp views for queries for menu choice 1
        self.sales_person_df.createOrReplaceTempView("sales_person")
        self.sales_territory_df.createOrReplaceTempView("sales_territory")
        self.employee_df.createOrReplaceTempView("employee")
        self.person_df.createOrReplaceTempView("person")

        #create a temporary view for the DataFrame for menu choice 2
        self.tbl_constraint_df.createOrReplaceTempView("tbl_constraint")

    #function that generates a dataframe for SalesYTD for Salesperson by Territory
    def salesytd_query(self, save_to_file=False, output_directory=None, output_file=None):
        query = """
                    SELECT
                        sp.BusinessEntityID
                        ,CONCAT(p.FirstName, COALESCE(CONCAT(' ', p.MiddleName), ''), ' ', p.LastName) as FullName
                        ,COALESCE(st.Name, 'No Territory') as SalesTerritory
                        ,format_number(sp.SalesYTD, 2) as SalesYTD
                        ,rank() OVER (ORDER BY sp.SalesYTD DESC) as OverallRank
                        ,rank() OVER (PARTITION BY st.Name ORDER BY sp.SalesYTD DESC) as TerritoryRank
                        ,format_number(percent_rank() OVER (ORDER BY sp.SalesYTD), 2) * 100 as PercentRank
                        ,format_number(lag(sp.SalesYTD) OVER (ORDER BY sp.SalesYTD), 2) as LagValue
                        ,lag(sp.BusinessEntityID) OVER (ORDER BY sp.SalesYTD) as LagID
                        ,format_number(lead(sp.SalesYTD) OVER (ORDER BY sp.SalesYTD), 2) as LeadValue
                        ,lead(sp.BusinessEntityID) OVER (ORDER BY sp.SalesYTD) as LeadID
                    FROM
                        sales_person sp
                    LEFT JOIN
                        sales_territory st ON st.TerritoryID = sp.TerritoryID
                    JOIN
                        employee e ON e.BusinessEntityID = sp.BusinessEntityID
                    JOIN
                        person p ON p.BusinessEntityID = e.BusinessEntityID
                    ORDER BY OverallRank ASC
                """
        result_df = self.spark.sql(query)
        result_df.show()

        if save_to_file:
            if output_directory is None or output_file is None:
                raise ValueError("Both output_directory and output_file must be specified when save_to_file is True.")

            #create an output directory if it does not exist
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)

            #save the query result as a CSV file
            output_path = os.path.join(output_directory, output_file)
            result_df.write.csv(output_path, header=True)

    #generates a dataframe with the constraints for the AdventureWorks database
    def tbl_constraint_query(self):
        query = """
                    SELECT * 
                    FROM tbl_constraint
                """

        # Execute the SQL query
        result_df = self.spark.sql(query)
        result_df.show(truncate=False)

    #stops the spark connection
    def close(self):
        # Stop the SparkSession
        self.spark.stop()

    #function to call the spark menu
    def spark_menu(self):
        while True:
            print("\n-------------------------Spark Menu-----------------------------")
            print("[1] Run a query to generate a SalesYTD report")
            print("[2] Run a query to output constraints for the AdventureWorks database")
            print("[3] Exit")
            print("-------------------------------------------------------------------")
            choice = input("Enter your choice: ")

            if choice == '1':
                output_directory = "Output/SalesPersonYTDRank_number2"
                output_file = "SalesPersonYTDRank_number2.csv"

                self.salesytd_query()

                # Prompt the user to save the output file
                save_output = input("Save the query result to a CSV file? [Y/N]: ")
                if save_output.upper() == "Y":
                    self.salesytd_query(save_to_file=True, output_directory=output_directory, output_file=output_file)

            elif choice == '2':
                self.tbl_constraint_query()

            elif choice == '3':
                self.close()
                print("Spark Session closed...")
                break

            else:
                print("Invalid choice. Please try again.")




