#### Narrow and Wider Transformations :
data = ["Project",
"Gutenberg’s",
"Alice’s",
"Adventures",
"in",
"Wonderland",
"Project",
"Gutenberg’s",
"Adventures",
"in",
"Wonderland",
"Project",
"Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)

###Map:
rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)


#### reducebykey #######

rdd3=rdd2.reduceByKey(lambda x,y:x+y)
for element in rdd3.collect():
    print(element)

### groupByKey ######
rdd=spark.sparkContext.textFile("file:///content/sample_data/avg_rent.csv")
rdd.collect()

rdd2=rdd.map(lambda x:x.split(','))
rdd2.collect()

rdd2.groupByKey().collect()

#### Reduce By Key ######

rdd4=rdd2.reduceByKey(lambda x,y:x+y)

for a in rdd4.collect():
    print(a)
	

#### Sort by Key ########

rdd5=rdd4.map(lambda x:(x[0],x[1])).sortByKey()
rdd5.collect()

rdd6=rdd5.filter(lambda x:'seegehalli' in x[0])

rdd6.collect()


##### Broadcast variable #####
sc=spark.sparkContext
test = sc.parallelize([(1),(2),(3),(4)]).zipWithIndex().map(lambda x: (x[1],x[0]))
test.collect()
test = sc.broadcast(test.collectAsMap())
print(test.value[0])


def modify_broadcast(j,test):
  main=j[0]
  context=j[1]
  test.value[main]=test.value[main]+1
  test.value[context]=test.value[context]+1
  return test.value

  coocurence = sc.parallelize([(0,1),(1,2),(3,2)]).map(lambda x: modify_broadcast(x,test))	
