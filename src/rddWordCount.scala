import org.apache.spark.SparkContext

object rddWordCount extends App {
      // Setting up spark context
	    val sc = new SparkContext("local[*]", "wordcount")

	    // Loading the file
			val rdd1 = sc.textFile("file:///D:\\BIG DATA COURSE\\file1.txt")
			
			//applying transformations
			//mapping one to one using anonymous function
			val rdd3 = rdd1.map(x => (x,1))

			//applying transformations
			val rdd4 = rdd3.reduceByKey((a,b) => a+b)

			// performing an action
			rdd4.collect.foreach(println)

			// to avoid the termination of DAG UI view
			scala.io.StdIn.readLine()




}