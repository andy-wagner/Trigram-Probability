import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProbCalculator {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, CounterType> {
        private Text word = new Text();
        private CounterType counter = new CounterType();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String stringValue = value.toString();
            if (stringValue.contains("c0")) {
                long count = Long.parseLong(stringValue.split("c0")[1].trim());
                context.getCounter(StaticVars.Counter.COUNTER_0).setValue(count);
            }
            else if (stringValue.contains(":")) {
                String[] Pair = stringValue.split("\t");
                Text newWord = new Text(Pair[0].trim()); // the full trigram
                String[] counterValue = Pair[1].split(" : ");
                long count = Long.parseLong(counterValue[0].trim());
                String part_of_word = counterValue[1].trim(); // the part of the trigram
                word.set(newWord);
                counter.setText(new Text(part_of_word));
                counter.setCount(new LongWritable(count));
                System.out.println("key: "+word+" value: "+counter);
                context.write(word, counter);
            }
        }
    }
    public static class ReducerClass extends Reducer<Text, CounterType, Text, DoubleWritable> {
        private Text trigram = new Text();
        private String[] splitTrigram;
        private double N3;
        private double N2;
        private double C2;
        private double N1;
        private double C1;
        private long C0;
        private double K2;
        private double K3;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            C0 = Long.parseLong(counterReader());
        }

        @Override
        public void reduce(Text key, Iterable<CounterType> values, Context context) throws IOException, InterruptedException {
                trigram.set(key);
                splitTrigram = trigram.toString().trim().split(" ");
                long count = 0;
                for (CounterType value : values) {
                    count++;
                    String counterInString = value.getText().toString();
                    String[] splitCounterString = counterInString.trim().split(" ");
                    System.out.println("trigram: " + trigram + " counter: " + counterInString);
                    if (splitCounterString.length == 3) {
                        if (is_N3(splitCounterString)) {
                            N3 = value.getCount().get();
                        }
                    }
                    else if (splitCounterString.length == 2) {
                        if (is_N2(splitCounterString)) {
                            N2 = value.getCount().get();
                        }
                        if (is_C2(splitCounterString)) {
                            C2 = value.getCount().get();
                        }
                    }
                    else if (splitCounterString.length == 1) {
                        if (is_C1(splitCounterString)) {
                            C1 = value.getCount().get();
                        }
                        if (is_N1(splitCounterString)) {
                            N1 = value.getCount().get();
                        }
                    }
                    else {
                        throw new Error(counterInString+" doesn't make any sense");
                    }
                }
                System.out.println(count);
                K2 = compute_K(2);
                K3 = compute_K(3);
                System.out.println(trigram.toString()+": C0-"+C0+" C1-"+C1+" C2-"+C2+" N1-"+N1+" N2-"+N2+" N3-"+N3+" K2-"+K2+" K3-"+K3);
            DoubleWritable probability = compute_probability();
                context.write(trigram, probability);
        }

        private DoubleWritable compute_probability() {
            double firstPart = K3*(N3/C2);
            double secondPart = (1-K3)*K2*(N2/C1);
            double thirdPart = (1-K3)*(1-K2)*(N1/C0);
            System.out.println("first part: "+firstPart+" second part: "+secondPart+" third part: "+thirdPart);
            return new DoubleWritable(firstPart + secondPart + thirdPart);
        }

        private double compute_K(int k_num) {
            double N_num = 0;
            if (k_num == 2) N_num = N2;
            else if (k_num == 3) N_num = N3;
            double numerator = Math.log(N_num+1)+1;
            double denominator = numerator + 1;
            return numerator/denominator;
        }

        private boolean is_N3(String[] splitCounterString) {
            return splitTrigram[0].equals(splitCounterString[0]) &&
                    splitTrigram[1].equals(splitCounterString[1]) &&
                    splitTrigram[2].equals(splitCounterString[2]);
        }

        private boolean is_N2(String[] splitCounterString) {
            return splitTrigram[1].equals(splitCounterString[0]) &&
                    splitTrigram[2].equals(splitCounterString[1]);
        }

        private boolean is_C2(String[] splitCounterString) {
            return splitTrigram[0].equals(splitCounterString[0]) &&
                    splitTrigram[1].equals(splitCounterString[1]);
        }

        private boolean is_N1(String[] splitCounterString) {
            return splitTrigram[2].equals(splitCounterString[0]);
        }

        private boolean is_C1(String[] splitCounterString) {
            return splitTrigram[1].equals(splitCounterString[0]);
        }
    }

    public static class CombinerClass extends Reducer<Text, CounterType, Text, CounterType> {
        @Override
        protected void reduce(Text key, Iterable<CounterType> values, Context context) throws IOException, InterruptedException {
            for (CounterType value: values) {
                context.write(key, value);
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
        Job job = Job.getInstance();
        job.setJarByClass(ProbCalculator.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CounterType.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        if (job.waitForCompletion(true)) {
            //save the counter in s3
            System.exit(0);
        }
        System.exit(1);
    }

    private static String counterReader(){
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        S3Object object = s3.getObject(new GetObjectRequest(StaticVars.OUTPUT_BUCKET_NAME, "counter.txt"));
        InputStream content = object.getObjectContent();
        Scanner s = new Scanner(content).useDelimiter("\\A");
        String result = s.hasNext() ? s.next() : "";
        return result;
    }
}