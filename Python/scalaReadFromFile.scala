//Dosyadan Okuma
import scala.io.Source
  val temp=11;
    if (temp > 0) {
  
      for (line <- Source.fromFile("/home/beyhan/Desktop/SCRIPTT.txt").getLines)
	{
        print(line.length +" "+ line)
	temp=temp-1
    }
    else
      Console.err.println("Please enter filename")



// val lines = Source.fromFile("/home/beyhan/Desktop/Scriptler/Database/ObjectOps.sql").getLines.toList



import scala.io.Source
  
    def widthOfLength(s: String) = s.length.toString.length
  
      val lines = Source.fromFile("/home/beyhan/Desktop/Scriptler/Database/ObjectOps.sql").getLines.toList
  
      val longestLine = lines.reduceLeft(
        (a, b) => if (a.length > b.length) a else b 
      ) 
      val maxWidth = widthOfLength(longestLine)
  
      for (line <- lines) {
        val numSpaces = maxWidth - widthOfLength(line)
        val padding = " " * numSpaces
        print(padding + line.length +" | "+ line)
      }


