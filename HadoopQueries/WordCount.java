import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static enum Counter {
        RECORD_COUNT
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
         private final static IntWritable one = new IntWritable(1);
         private Text word = new Text();
         private int lineNumber = 0;

         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header row
            if (lineNumber == 0) {
                lineNumber++;
                return;
            }
           String[] fields = value.toString().split(",");

            // Process each field as a word
            for (String field : fields) {
                word.set(field.trim());
                context.write(word, one);
            }
            lineNumber++;
            context.getCounter(Counter.RECORD_COUNT).increment(1);
         }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
         private IntWritable result = new IntWritable();

         public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
             int sum = 0;
             for (IntWritable val : values) {
                  sum += val.get();
             }
             result.set(sum);
             context.write(key, result);
         }
    }



    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean jobStatus = job.waitForCompletion(true);
        long endTime = System.nanoTime();
        double duration = (endTime - startTime) / 1e9d;
        System.out.println("Execution Time: " + duration + " seconds");
        long recordCount = job.getCounters().findCounter(Counter.RECORD_COUNT).getValue();
        double throughput = recordCount / duration;
        System.out.println("Throughput: " + throughput + " records per second");

        Runtime runtime = Runtime.getRuntime();
        double usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0);
        System.out.println("Used Memory: " + usedMemory + " MB");

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        double usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0);
        double usedNonHeapMemory = memoryMXBean.getNonHeapMemoryUsage().getUsed() / (1024.0 * 1024.0);
        System.out.println("Used Heap Memory: " + usedHeapMemory + " MB");
        System.out.println("Used Non-Heap Memory: " + usedNonHeapMemory + " MB");

        System.exit(jobStatus ? 0 : 1);
     }
}
