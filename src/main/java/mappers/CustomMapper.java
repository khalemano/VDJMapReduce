package mappers;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import outputValues.ScoreKeeperOutput;

public class CustomMapper
        extends Mapper<Object, Text, Text, ScoreKeeperOutput> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        ScoreKeeperOutput muts = new ScoreKeeperOutput();

        String[] tokens = value.toString().split("\n");

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String file = fileSplit.getPath().getName();

        String name = tokens[2];
        name = file + "," + name;
        char[] query = tokens[1].toCharArray();
        char[] reference = tokens[3].toCharArray();

        for (int i = 0; i < reference.length; i++) {
            if (query[i] != reference[i]) {
                muts.addToCounter("mutations", 1);
            }
        }
        context.write(new Text(name), muts);

    }
}
