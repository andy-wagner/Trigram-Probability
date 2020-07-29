
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PairCount {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, CounterType> {
        private Text word1 = new Text();
        private Text word2 = new Text();
        private CounterType counter = new CounterType();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //counts words
            String[] splitInput = value.toString().split("\t");

            String occurrences = splitInput[2];
            String [] splitTrigram= splitInput[0].trim().split(" ");
            if (splitTrigram.length == 2 && checkIfLegalWord(splitInput[0])) {
                counter = new CounterType("empty", Long.parseLong(occurrences));
                word1.set(splitInput[0]);
                context.write(word1, counter);
            }
            else {
                counter = new CounterType(splitInput[0].trim(), -1);

                if (splitTrigram.length == 3 && checkIfLegalWord(splitInput[0])) {

                    // map w1w2
                    word1.set(splitTrigram[0] + " " + splitTrigram[1]);
                    context.write(word1, counter);

                    //map w2w3
                    word2.set(splitTrigram[1] + " " + splitTrigram[2]);
                    context.write(word2, counter);
                }
            }

        }
    }

    public static class ReducerClass extends Reducer<Text, CounterType, Text, CounterType> {
        private CounterType counter = new CounterType();

        public void reduce(Text key, Iterable<CounterType> values, Context context) throws IOException, InterruptedException {
            LongWritable count = new LongWritable();
            long countTmp = 0;
            LinkedList<String> newKey = new LinkedList<String>();

            for (CounterType value : values) {
                long valueCount = value.getCount().get();
                if (valueCount > 0) {   // if the count comes from 2-gram
                    countTmp += valueCount;
                }
                else if(!newKey.contains(value.getText().toString())) {  // if the count comes from the trigram
                    newKey.add(value.getText().toString());         // in which case the text is relevant
                }
            }
            count.set(countTmp);
            counter.setCount(count);
            counter.setText(key);

            for(int i=0 ; i< newKey.size();i++) {
                context.write(new Text(newKey.get(i)), counter);
            }

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
        job.setJarByClass(PairCount.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CounterType.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CounterType.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
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