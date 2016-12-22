from pyspark import SparkContext

logFile = "any_directory/README.md"  # Should be some file on your system
sc = SparkContext("local", "Beyhan's App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))


-->>save like   sparkDeneme.py on a file.
-*-*-*-*-*-*-*-*-*-*


When in this file, run it.

/usr/local/spark/bin/spark-submit \
  --master local[*] \
  sparkDeneme.py

------------------------- BEFORE OPEN SPARK SHELL
