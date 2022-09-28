rdd = sc.parallelize([(u'2', u'100', 2),(u'1', u'300', 1),(u'1', u'200', 1)])
rdd1 = sc.parallelize([(u'1', u'2'), (u'1', u'3')])
newRdd = rdd1.map(lambda x:(x[1],x[0])).join(rdd.map(lambda x:(x[0],(x[1],x[2]))))
newRdd.map(lambda x:(x[1][0], x[0], x[1][1][0], x[1][1][1])).coalesce(1).collect()



test1 = [('a', 20), ('b', 10), ('c', 2)]
test2 = [('a', 2), ('b', 3)]
test1.join(test2).map(lambda (key, values): (key,) + values).collect()


18,87,436

DataFrame : 

Data Frames is nothing but RDD with structure.

Data Frame can be created on any data set which have structure associated with it.
Attributes/columns in a data frame can be referred using names.
One can create data frame using data from files, hive tables, relational tables over JDBC.
Common functions on Data Frames
printSchema – to print the column names and data types of data frame
show – to preview data (default 20 records)
describe – to understand characteristics of data
count – to get number of records
collect – to convert data frame into Array
Once data frame is created, we can process data using 2 approaches.
Native Data Frame APIs
Register as temp table and run queries using spark.sql
To work with Data Frames as well as Spark SQL, we need to create object of type SparkSession

Examples:

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
df = spark.read.option("header","true").csv("/content/sample_data/departments.csv")
df.printSchema()
df.show()
df.count()
df.select("dept_no").show()

df.limit(2).show()df_sample = spark.read.csv("/content/sample_data/california_housing_test.csv", inferSchema = True, header = True)
df_sample.printSchema()

df2 = spark.read.csv("/content/sample_data/california_housing_test.csv",header = True)
df2.printSchema()

from pyspark.sql import *
 
Employee = Row("firstName", "lastName", "email", "salary")
 
employee1 = Employee('Basher', 'armbrust', 'bash@edureka.co', 100000)
employee2 = Employee('Daniel', 'meng', 'daniel@stanford.edu', 120000 )
employee3 = Employee('Muriel', None, 'muriel@waterloo.edu', 140000 )
employee4 = Employee('Rachel', 'wendell', 'rach_3@edureka.co', 160000 )
employee5 = Employee('Zach', 'galifianakis', 'zach_g@edureka.co', 160000 )

print(Employee[0])
 
print(employee3)

department1 = Row(id='123456', name='HR')
department2 = Row(id='789012', name='OPS')
department3 = Row(id='345678', name='FN')
department4 = Row(id='901234', name='DEV')

departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2, employee5])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee1, employee4, employee3])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

departmentsWithEmployees_Seq = [departmentWithEmployees1, departmentWithEmployees2]
dframe = spark.createDataFrame(departmentsWithEmployees_Seq)
display(dframe)
dframe.show(truncate=False)

df_sample.describe()
df_sample.printSchema()

col=df_sample.columns

for c in col:
  print(c.capitalize())

df_sample.select("longitude").describe().show()
df_sample.describe().show()


### Dataframe to RDD
rdd=df.rdd
rdd.collect()



df.select("dept_no").show()