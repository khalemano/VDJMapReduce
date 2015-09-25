package inputFormats;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import recordReaders.CustomRecordReader;

public class CustomInputFormat extends TextInputFormat{
    
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit,
            JobConf jobConf, Reporter reporter) throws IOException {
        return new CustomRecordReader((FileSplit) inputSplit, jobConf);
    }
}
