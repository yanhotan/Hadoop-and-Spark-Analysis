  GNU nano 2.0.9                                                                                                                                          File: AvgOrderQtyByCategory.java                                                                                                                                                                                                                                                                                           

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgOrderQtyByCategory {

public static enum Counter {
RECORD_COUNT
}

public static class AvgOrderQtyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
private Text categoryName = new Text();
private DoubleWritable orderQty = new DoubleWritable();
private int lineNumber = 0;

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
// Skip the header row
if (lineNumber == 0) {
lineNumber++;
return;
}

String[] fields = value.toString().split(",");
try {
// Assuming category_name is at index 12 and avg_order_qty is at index 4
String category = fields[12].trim();
double quantity = Double.parseDouble(fields[4].trim());

categoryName.set(category);
orderQty.set(quantity);
context.write(categoryName, orderQty);

context.getCounter(Counter.RECORD_COUNT).increment(1);
} catch (Exception e) {
System.err.println("Error processing record: " + value.toString() + " - " + e.getMessage());
}
lineNumber++;
}
}

public static class AvgOrderQtyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
private DoubleWritable averageQty = new DoubleWritable();

public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
double sum = 0;
int count = 0;
for (DoubleWritable val : values) {
sum += val.get();
count++;
}
averageQty.set(sum / count);
context.write(key, averageQty);
}
}

public static void main(String[] args) throws Exception {
long startTime = System.nanoTime();
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Average Order Quantity by Category");
job.setJarByClass(AvgOrderQtyByCategory.class);
job.setMapperClass(AvgOrderQtyMapper.class);
job.setCombinerClass(AvgOrderQtyReducer.class);
job.setReducerClass(AvgOrderQtyReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(DoubleWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

boolean jobStatus = job.waitForCompletion(true);
long endTime = System.nanoTime();
double duration = (endTime - startTime) / 1e9d;
System.out.println("Execution Time: " + duration + " seconds");
long recordCount = job.getCounters().findCounter(Counter.RECORD_COUNT).getValue();
double throughput = recordCount / duration;
System.out.println("Throughput: " + throughput + " records per second");

MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
double usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0);
double usedNonHeapMemory = memoryMXBean.getNonHeapMemoryUsage().getUsed() / (1024.0 * 1024.0);
System.out.println("Used Heap Memory: " + usedHeapMemory + " MB");
System.out.println("Used Non-Heap Memory: " + usedNonHeapMemory + " MB");

System.exit(jobStatus ? 0 : 1);
}
}

