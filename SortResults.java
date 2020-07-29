import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortResults {

    public static class MapperClass extends Mapper<LongWritable, Text, ProbKey, Text> {
        private Text grams = new Text();
        private Text w1w2 = new Text();
        private DoubleWritable prob = new DoubleWritable();
        private ProbKey sortKey = new ProbKey();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] args = value.toString().split("\t");
            String[] word = args[0].split(" ");
            String w1 = word[0];
            String w2 = word[1];
            String w3 = word[2];

            prob.set(Double.parseDouble(args[1]));
            w1w2.set(w1+w2);
            grams.set(w1 +" "+w2 + " "+w3);

            sortKey.set(w1w2, prob);
            context.write(sortKey, grams);
        }
    }

    public static class ReducerClass extends Reducer<ProbKey, Text, Text, DoubleWritable> {
        @Override
        public void reduce(ProbKey key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            Text grams = new Text();
            for (Text value : values) {
                grams.set(value.toString());
                context.write(grams, key.getProb());
            }
        }
    }

    public static class PartitionerClass extends Partitioner<ProbKey, Text> {
        @Override
        public int getPartition(ProbKey key, Text value, int numPartitions) {
            return (Math.abs(key.hashCode()) % numPartitions);
        }
    }

    public static class KeyComparator extends WritableComparator {

        public KeyComparator() {
            super(ProbKey.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {

            ProbKey key1 = (ProbKey) wc1;
            ProbKey key2 = (ProbKey) wc2;
            return key1.compareTo(key2);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(SortResults.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setGroupingComparatorClass(KeyComparator.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setMapOutputKeyClass(ProbKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0:1);
    }

    private static class ProbKey implements WritableComparable<ProbKey> {
        Text text;
        DoubleWritable prob;

        //Default Constructor
        public ProbKey() {
            this.text = new Text();
            this.prob = new DoubleWritable();
        }

        public void set(Text text, DoubleWritable prob) {
            this.text = text;
            this.prob = prob;
        }

        public Text getText() {
            return text;
        }

        public DoubleWritable getProb() {
            return prob;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            text.write(dataOutput);
            prob.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            text.readFields(dataInput);
            prob.readFields(dataInput);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof ProbKey){
                ProbKey other = (ProbKey) o;
                return (this.compareTo(other) == 0);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return text.hashCode() + prob.hashCode();
        }

        @Override
        public String toString(){
            return text.toString() +" : "+ prob.toString();
        }

        @Override
        public int compareTo(ProbKey other) {
            int wordCompare = this.getText().compareTo(other.getText());
            if (wordCompare != 0)
                return wordCompare;
            return (-1) * prob.compareTo(other.getProb());
        }
    }
}