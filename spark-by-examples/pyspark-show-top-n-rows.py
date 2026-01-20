# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James",34),("Ann",34),
    ("Michael",33),("Scott",53),
    ("Robert",37),("Chad",27)
  ]

columns = ["firstname","age",]
df = spark.createDataFrame(data = simpleData, schema = columns)


df.show()
#Returns the first ``num`` rows as a :class:`list` of :class:`Row`.
# Internally calls limit and collect
#Action, Return Array[T]
print(df.take(2))

#Returns the last ``num`` rows as a :class:`list` of :class:`Row`.
#Running tail requires moving data into the application's driver process, and doing so with
#a very large ``num`` can crash the driver process with OutOfMemoryError.
#Return Array[T]
print(df.tail(2))


"""Returns the first ``n`` rows.

.. note:: This method should only be used if the resulting array is expected
    to be small, as all the data is loaded into the driver's memory.

:param n: int, default 1. Number of rows to return.
:return: If n is greater than 1, return a list of :class:`Row`.
    If n is 1, return a single Row."""
#Return Array[T]
print(df.head(2))


#Returns the first row, same as df.head(1)
print(df.first())

#Returns all the records as a list of :class:`Row`.
#Action, Return Array[T]
print(df.collect())
#"Limits the result count to the number specified.
#Returns a new Dataset by taking the first n rows.
pandasDF=df.limit(3).toPandas()
print(pandasDF)

