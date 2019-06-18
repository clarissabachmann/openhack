# Databricks notebook source
from pyspark.sql.functions import lit
from datetime import datetime
from pyspark.sql.functions import *

df1 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLInputSales/Addresses.txt", header = True)
df2 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLInputSales/Customers.txt", header = True)
df3 = df1.join(df2, df1.CustomerID == df2.CustomerID).select(df1.CustomerID, df2.LastName, df2.FirstName, df1.AddressID, df1.AddressLine1, df1.AddressLine2, df1.City, df1.State, df1.ZipCode, df2.PhoneNumber, df1.CreatedDate, df1.UpdatedDate)
df3 = df3.withColumn('SourceID', lit("1"))
df3 = df3.withColumn('UniqueID', concat(df3.SourceID, df3.CustomerID, df3.AddressID))
#df3 = df3.select(df3.CustomerID, df3.LastName, df3.FirstName, df3.AddressLine1, df3.Address.Line2, df3.City, df3.State, df3.ZipCode, df3.PhoneNumber, df3.CreatedDate, df3.UpdatedDate)


df2 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLInputStreaming/Customers.txt", header = True)
df1 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLInputStreaming/Addresses.txt", header = True)
df4 = df1.join(df2, df1.CustomerID == df2.CustomerID).select(df1.CustomerID, df2.LastName, df2.FirstName, df1.AddressID, df1.AddressLine1, df1.AddressLine2, df1.City, df1.State, df1.ZipCode, df2.PhoneNumber, df1.CreatedDate, df1.UpdatedDate)
df4 = df4.withColumn('SourceID', lit(1))
df4 = df4.withColumn('UniqueID', concat(df4.SourceID, df4.CustomerID, df4.AddressID))

df5 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLVanArsdellInput/Customers.txt", header = True)
df5 = df5.withColumn('SourceID', lit(3)).withColumn('AddressID', lit('NULL'))
df5 = df5.withColumn('UniqueID', (df5.SourceID + df5.CustomerID + df5.AddressID))

df6 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/FolderFourthCoffeeInput/Customers.csv", header = True)
df6 = df6.withColumn('SourceID', lit(2)).withColumn('AddressID', lit('NULL'))
df6 = df6.withColumn('UniqueID', concat(df6.SourceID + df6.CustomerID + df6.AddressID))


unionDF = df3.unionAll(df4)
unionDF1 = df5.unionAll(df6)
df1 = unionDF.unionAll(unionDF1)
Customerdf = df1.select(df1.SourceID.cast('int'), df1.UniqueID.cast('string'), df1.AddressID, df1.CustomerID, df1.LastName, df1.FirstName, df1.AddressLine1, df1.AddressLine2, df1.City, df1.State, df1.ZipCode, df1.PhoneNumber, df1.CreatedDate.cast('timestamp'), df1.UpdatedDate.cast('timestamp'))
Customerdf = Customerdf.coalesce(1)
#display(Customerdf)

Customerdf.write.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/OutputCSVs/CustomersCombined.csv")
                     

# COMMAND ----------

df1 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLInputStreaming/Customers.txt", header = True)
df2 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLInputStreaming/Transactions.txt", header = True)
df3 = df1.join(df2, 'CustomerID')

df3 = df3.withColumn('SourceID', lit("1"))
df3 = df3.withColumn('UniqueOrderID', concat(df3.SourceID, df3.TransactionID))
df3 = df3.withColumn('UniqueMovieID', concat(df3.SourceID, df3.MovieID))
df3 = df3.withColumn('UniqueCustomerID', concat(df3.SourceID, df3.TransactionID))
df3 = df3.select(df3.SourceID.cast('int'), df3.UniqueOrderID, df3.TransactionID, df3.UniqueMovieID, df3.MovieID, df3.CustomerID, df3.UniqueCustomerID, df3.StreamStart.cast('timestamp'), df3.StreamEnd.cast('timestamp'))

Customerdf.write.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/OutputCSVs/StreamingCombined.csv")
display(df3)


# COMMAND ----------

