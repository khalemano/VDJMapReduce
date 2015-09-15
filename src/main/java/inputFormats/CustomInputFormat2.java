/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package inputFormats;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import recordReaders.CustomRecordReader2;

/**
 *
 * @author kalanihalemano
 */
public class CustomInputFormat2 extends FileInputFormat <LongWritable,Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            CustomRecordReader2 recordReader = new CustomRecordReader2();
//            recordReader.initialize(is, tac);
            return recordReader;
    }
    
}
