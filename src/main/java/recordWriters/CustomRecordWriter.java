package recordWriters;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import outputValues.ScoreKeeperOutput;

public class CustomRecordWriter extends RecordWriter<Text, ScoreKeeperOutput> {

    private DataOutputStream out;
    private boolean firstRecord;

    public CustomRecordWriter(DataOutputStream stream) {
        out = stream;
        firstRecord = true;
        try {
            out.writeBytes("");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void write(Text k, ScoreKeeperOutput v) throws IOException, InterruptedException {
        Set<String> keys = v.getKeys();
        if (firstRecord){
            out.writeBytes("file,name");
            for (String key:keys){
                out.writeBytes(","+key);
            }
            out.writeBytes("\n");
            firstRecord=false;
        }
        out.writeBytes(k.toString());
        for (String key:keys){
            out.writeBytes("," + v.getValue(key));
        }
        out.writeBytes("\n");
    }

    @Override
    public void close(TaskAttemptContext tac) throws IOException, InterruptedException {
        out.close();
    }

}
