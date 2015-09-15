/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package outputFormats;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import outputValues.ScoreKeeperOutput;
import recordWriters.CustomRecordWriter;

/**
 *
 * @author kalanihalemano
 */
public class CustomOutputFormat extends FileOutputFormat<Text,ScoreKeeperOutput>{

    @Override
    public RecordWriter<Text, ScoreKeeperOutput> getRecordWriter(TaskAttemptContext tac) throws IOException, InterruptedException {

        Path path = FileOutputFormat.getOutputPath(tac);
        
        Path fullpath = new Path(path,"result.txt");
        
        FileSystem fs = fullpath.getFileSystem(tac.getConfiguration());
        
        FSDataOutputStream fileOut = fs.create(fullpath,tac);
        
        return new CustomRecordWriter(fileOut);
    }
    
}
