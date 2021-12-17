# Databricks notebook source
# MAGIC %md ### Transformations and Actions

# COMMAND ----------

# Parallelize - Create RDD
rdd1 = sc.parallelize(["a", "b", "c", "d", "e"])

# COMMAND ----------

result = rdd1.collect()

print(result)

# COMMAND ----------

# Check number of partitions
rdd1.getNumPartitions()

# COMMAND ----------

rdd1.first()

# COMMAND ----------

# EXERCISE - GIVE ANY 2 RECORDS

# COMMAND ----------

# Map - Create word pairs
data = ["John", "Fred", "Anna", "James", "Mohit"]

words = sc.parallelize(data)

wordPairs = words.map(lambda word: (word[0], word))            

for element in wordPairs.collect():
    print(element)

# COMMAND ----------

# Filter - Find even numbers

numbers = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

#numbers.getNumPartitions()

evenNumbers = numbers.filter(lambda x: x % 2 == 0)

#evenNumbers.getNumPartitions()

# COMMAND ----------

result = evenNumbers.collect()

print(result)

# COMMAND ----------

# EXERCISE - GIVE MAX VALUE FROM THE LIST

# COMMAND ----------

data = ["John", "Fred", "Anna", "James", "Mohit"]

# EXERCISE - FIND RECORDS WITH NAME = "Anna"
# EXERCISE - FIND RECORDS WITH NAMES STARTING WITH "J"

# COMMAND ----------

# MAGIC %md ###Types of Transformations
# MAGIC 
# MAGIC  - Check which transformations are narrow and which are wide
# MAGIC  - Check for jobs / stages / tasks

# COMMAND ----------

# Map - Create word pairs
data = ["John", "Fred", "Anna", "James", "Mohit"]

words = sc.parallelize(data)

wordPairs = words.map(lambda word: (word[0], word))            

print(wordPairs.collect())

# COMMAND ----------

wordPairs.getNumPartitions()

# COMMAND ----------

# Filter - Find even numbers

numbers = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

#numbers.getNumPartitions()

evenNumbers = numbers.filter(lambda x: x % 2 == 0)

evenNumbers.collect()

#evenNumbers.getNumPartitions()

# COMMAND ----------

# Distinct - Find distinct elements
words = sc.parallelize(["John", "Fred", "Anna", "John", "Mohit", "Anna"])

distinct = words.distinct()

distinct.collect()

# COMMAND ----------

# Union - Get all elements and check Job View
words1 = sc.parallelize(["John", "Fred", "Anna", "James", "Mohit"])
words2 = sc.parallelize(["Mohit", "Anna", "Richard"])

words1.getNumPartitions()
words2.getNumPartitions()

common = words1.union(words2)

common.getNumPartitions()

common.collect()

# COMMAND ----------

# Find word count
data = ["John", "Fred", "Anna", "James", "Mohit"]

words = sc.parallelize(data)

wordPairs = words.map(lambda word: (word[0], word))

wordCounts = wordPairs.groupByKey()
                  
wordCounts.collect()

# COMMAND ----------

# Actions 'first', 'count'
words = sc.parallelize(["John", "Fred", "Anna", "John", "Mohit", "Anna"])

print(words.first())

print(words.count())
