text_file = sc.textFile("/home/ubuntu/testFile.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) 
             .map(lambda word: (word, 1)) 
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("/home/ubuntu/results.txt")

