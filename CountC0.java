import java.io.File;
import java.io.IOException;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountC0 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //counts words
            String[] splitInput = value.toString().split("\t");

            String occurrences = splitInput[2];

            if (checkIfLegalWord(splitInput[0])) {
                context.write(new Text(key.toString()), new LongWritable(Long.parseLong(occurrences)));
            }


        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            Counters.Counter c = (Counters.Counter) context.getCounter(StaticVars.Counter.COUNTER_0);
            c.increment(sum);
        }
    }

    public static class CombinerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return (int)(Math.random() * numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        for (String arg: args) System.out.println(arg);
        Job job = Job.getInstance();
        job.setJarByClass(CountC0.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        if (job.waitForCompletion(true)) {
            long counter = job.getCounters().findCounter(StaticVars.Counter.COUNTER_0).getValue();
//            save the counter in s3
            uploadFile(counter);
            System.exit(0);
        }
        System.exit(1);
    }

    private static void uploadFile(long c) {

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        try {
            FileUtils.writeStringToFile(new File("counter.txt"), String.valueOf(c), "UTF-8");
        } catch (IOException e) {
            System.out.println("Error: can not write counter to text");
        }

        s3.putObject(new PutObjectRequest(StaticVars.OUTPUT_BUCKET_NAME, "counter.txt", new File("counter.txt")));
    }


    private static boolean checkIfLegalWord(String word) {
        word = word.trim();
        boolean checkSpace = false;
        for (int i = 0; i < word.length(); i++) {
            if (((int) word.charAt(i) < 1488 || (int) word.charAt(i) > 1514) && (int) word.charAt(i) != 32)
                return false;
            if ((int) word.charAt(i) >= 1488 && (int) word.charAt(i) <= 1514) {
                checkSpace = true;
            }
        }
        return checkSpace;

    }
}