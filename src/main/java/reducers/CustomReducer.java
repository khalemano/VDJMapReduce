package reducers;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import outputValues.ScoreKeeperOutput;

public class CustomReducer
        extends Reducer<Text, ScoreKeeperOutput, Text, ScoreKeeperOutput> {


    public void reduce(Text key, Iterable<ScoreKeeperOutput> values,
            Context context) throws IOException, InterruptedException {
        
        ScoreKeeperOutput master = new ScoreKeeperOutput();
        for (ScoreKeeperOutput value : values) {
            for (String name: value.getKeys()){
                master.addToCounter(name, value.getValue(name));
            }
        }
         
        context.write(key, master);
    }
}
