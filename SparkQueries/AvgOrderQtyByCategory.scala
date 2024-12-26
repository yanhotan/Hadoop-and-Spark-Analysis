import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

// Load the data from the CSV file
val linesRDD = sc.textFile("hdfs:///user/cloudera/data/final_dataset.csv")

// Skip the header and split the lines
val header = linesRDD.first()
val dataRDD = linesRDD.filter(row => row != header).map(_.split(","))

// Extract category_name and avg_order_qty (fields[12] for category_name and fields[3] for avg_order_qty)
val categoryAndAvgQtyRDD: RDD[(String, String)] = dataRDD.map(fields => (fields(12).trim, fields(3).trim))

// Aggregate by category_name to calculate the average order quantity
val aggregatedRDD = categoryAndAvgQtyRDD
  .filter { case (_, qty) => qty.nonEmpty && qty.forall(_.isDigit) } // Filter valid quantities
  .mapValues(qty => (qty.toDouble, 1)) // Map to (category_name, (sum, count))
  .reduceByKey { case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2) } // Sum and count
  .mapValues { case (sum, count) => sum / count } // Calculate average

// Reorder columns: category_name on the left and avg_order_qty on the right
val orderedOutputRDD = aggregatedRDD.map { case (category, avgQty) => s"$category, $avgQty" }

// Save the output to HDFS
val outputPath = "hdfs:///user/cloudera/data/output_avg_order_qty"
val fs = FileSystem.get(sc.hadoopConfiguration)
val path = new Path(outputPath)

// Check if output path exists and delete it if necessary
if (fs.exists(path)) {
  fs.delete(path, true)
}

// Save the results line by line
orderedOutputRDD.saveAsTextFile(outputPath)

println(s"Output saved to: $outputPath")
