import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TrigramCount {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, CounterType> {
        private Text word = new Text();
        private CounterType counter = new CounterType();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splitInput = value.toString().split("\t");
            String[] splitTrigram = splitInput[0].trim().split(" ");

            if (splitTrigram.length == 3 && checkIfLegalWord(splitInput[0])) {
                word.set(splitInput[0]);
                String occurrences = splitInput[2];
                context.write(word, new CounterType(splitInput[0], Long.parseLong(occurrences)));
            }


        }
    }

        public static class ReducerClass extends Reducer<Text, CounterType, Text, CounterType> {
            public void reduce(Text key, Iterable<CounterType> values, Context context) throws IOException, InterruptedException {
                long sum = 0;
                for (CounterType value : values) {
                    sum += value.getCount().get();
                }

                context.write(key, new CounterType(key.toString(), sum));
            }
        }

    public static class PartitionerClass extends Partitioner<Text, CounterType> {
        @Override
        public int getPartition(Text key, CounterType value, int numPartitions) {
            return (Math.abs(key.hashCode()) % numPartitions);
        }
    }


    public static void main(String[] args) throws Exception {
        for (String arg: args) System.out.println(arg);
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(TrigramCount.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CounterType.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CounterType.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        if (job.waitForCompletion(true)) {
            System.exit(0);
        }
        System.exit(1);
    }

    private static boolean checkIfLegalWord(String word) {
        word = word.trim();

        for (int i = 0; i < word.length(); i++) {
            if(((int)word.charAt(i) < 1488 || (int)word.charAt(i) > 1514) && (int)word.charAt(i) != 32 )
                return false;
        }
        return true;

    }

}