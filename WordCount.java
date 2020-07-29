import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {

    public static class MapperClass extends Mapper<LongWritable, Text, PlaceText, CounterType> {
        private PlaceText word1 = new PlaceText(new IntWritable(0));
        private PlaceText word2 = new PlaceText(new IntWritable(1));
        private PlaceText word3 = new PlaceText(new IntWritable(2));
        private Text word = new Text();
        private CounterType counter = new CounterType();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //counts words
            String[] splitInput = value.toString().split("\t");

            String occurrences = splitInput[2];
            String[] splitTrigram = splitInput[0].trim().split(" ");

            if (splitTrigram.length == 1 && checkIfLegalWord(splitTrigram[0])) {
                counter = new CounterType("empty", Long.parseLong(occurrences));
                word.set(splitTrigram[0]);
                word1.setText(word);
                context.write(word1, counter);
                word2.setText(word);
                context.write(word2, counter);
                word3.setText(word);
                context.write(word3, counter);
            } else {
                counter = new CounterType(splitInput[0].trim(), -1);
                boolean checkSplits = true;
                for (String split : splitTrigram) {
                    checkSplits = checkSplits && checkIfLegalWord(split);
                }
                if (!checkSplits) {
                    System.out.println(1);
                }
                if (splitTrigram.length == 3 && checkSplits) {
                    // map w1
                    word.set(splitTrigram[0]);
                    word1.setText(word);
                    context.write(word1, counter);

                    //map w2
                    word.set(splitTrigram[1]);
                    word2.setText(word);
                    context.write(word2, counter);

                    //map w3
                    word.set(splitTrigram[2]);
                    word3.setText(word);
                    context.write(word3, counter);
                }
            }

        }
    }

    public static class ReducerClass extends Reducer<PlaceText, CounterType, Text, CounterType> {
        private CounterType counter = new CounterType();

        public void reduce(PlaceText key, Iterable<CounterType> values, Context context) throws IOException, InterruptedException {
            LongWritable sum = new LongWritable();
            long countTmp = 0;
            long check = 0;
            LinkedList<String> newKey = new LinkedList<String>();
            boolean hasTrigram = false;
            for (CounterType value : values) {
                check++;
                if (value.getCount().get() > 0) {
                    countTmp += value.getCount().get();
                } else if (!newKey.contains(value.getText().toString())) { // if the count comes from the trigram
                    newKey.add(value.getText().toString());             // in which case the text is relevant
                    hasTrigram = true;
                }
            }
            if (hasTrigram) {
                sum.set(countTmp);
                counter.setCount(sum);
                counter.setText(key.getText());

                for (String str : newKey) {
                    context.write(new Text(str), counter);
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<PlaceText, CounterType> {
        @Override
        public int getPartition(PlaceText key, CounterType value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        for (String arg : args) System.out.println(arg);
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(PlaceText.class);
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

    private static class PlaceText implements WritableComparable<PlaceText> {
        Text text;
        IntWritable place;

        public PlaceText() {
            this.text = new Text();
            this.place = new IntWritable();
        }

        public PlaceText(IntWritable place) {
            this.text = new Text();
            this.place = place;
        }

        public void setText(Text text) {
            this.text = text;
        }


        public Text getText() {
            return text;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            text.write(dataOutput);
            place.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            text.readFields(dataInput);
            place.readFields(dataInput);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof PlaceText){
                PlaceText other = (PlaceText) o;
                return (this.compareTo(other) == 0);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return text.hashCode() + place.hashCode();
        }

        @Override
        public String toString(){
            return text.toString() +" : "+ place.get();
        }

        @Override
        public int compareTo(PlaceText other) {
            int wordCompare = this.getText().compareTo(other.getText());
            if (wordCompare == 0)
                return this.place.get() - other.place.get();
            return wordCompare;
        }
    }
}
