/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package reducers;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import outputValues.ScoreKeeperOutput;

/**
 *
 * @author kalanihalemano
 */
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
