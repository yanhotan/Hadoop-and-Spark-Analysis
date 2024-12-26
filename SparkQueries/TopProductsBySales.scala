// Query: Top Product Count
import org.apache.hadoop.fs.{FileSystem, Path}
import java.lang.management.ManagementFactory
import java.lang.management.MemoryMXBean

// Load the data from the CSV file
val linesRDD = sc.textFile("hdfs:///user/cloudera/data/final_dataset.csv")
// Start timer for execution time measurement
val startTime = System.nanoTime()

// Skip the header and split the lines
val header = linesRDD.first()
val dataRDD = linesRDD.filter(row => row != header).map(_.split(","))

// Aggregate by product_name to calculate the count
val productCountRDD = dataRDD
  .map(fields => (fields(2).trim, 1)) // Extract product_name (fields[2] is product_name column) and map to 1
  .reduceByKey(_ + _) // Aggregate counts
  .sortBy(_._2, ascending = false) // Sort by count in descending order

// Format the output as "product_name, count"
val formattedOutputRDD = productCountRDD.map { case (product, count) => s"$product, $count" }

// Define the output path
val outputPath = "hdfs:///user/cloudera/data/output_top_product_count"
val fs = FileSystem.get(sc.hadoopConfiguration)
val path = new Path(outputPath)

// Check if output path exists and delete it if necessary
if (fs.exists(path)) {
  fs.delete(path, true)
}

// Save the results line by line
formattedOutputRDD.saveAsTextFile(outputPath)

// End timer and calculate duration
val endTime = System.nanoTime()
val durationInSeconds = (endTime - startTime) / 1e9d

// Count the total records processed
val totalRecords = dataRDD.count()

// Calculate throughput
val throughput = totalRecords / durationInSeconds

// Measure memory usage
val runtime = Runtime.getRuntime
val usedMemory = (runtime.totalMemory - runtime.freeMemory) / (1024.0 * 1024.0)
val memoryMXBean = ManagementFactory.getMemoryMXBean
val heapMemoryUsed = memoryMXBean.getHeapMemoryUsage.getUsed / (1024.0 * 1024.0)
val nonHeapMemoryUsed = memoryMXBean.getNonHeapMemoryUsage.getUsed / (1024.0 * 1024.0)

// Print metrics
println(s"Execution Time: $durationInSeconds seconds")
println(s"Throughput: $throughput records per second")
println(s"Used Memory: $usedMemory MB")
println(s"Heap Memory Used: $heapMemoryUsed MB")
println(s"Non-Heap Memory Used: $nonHeapMemoryUsed MB")

println(s"Output saved to: $outputPath")
