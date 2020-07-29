import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CounterType implements Writable {
    private Text text;
    private LongWritable count;

    public CounterType() {
        text = new Text();
        count = new LongWritable();
    }

    public CounterType(String text, long count) {
        this.text = new Text(text);
        this.count = new LongWritable(count);
    }

    public void setText(Text text) {
        this.text = text;
    }

    public void setCount(LongWritable count) {
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        count.write(out);
        text.write(out);
    }

    public void readFields(DataInput out) throws IOException {
        count.readFields(out);
        text.readFields(out);
    }

    public Text getText() {
        return text;
    }

    public LongWritable getCount() {
        return count;
    }

    @Override
    public String toString() {
        return (count.toString() + " : " + text.toString());
    }
}
