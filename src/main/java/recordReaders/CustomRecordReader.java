/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package recordReaders;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 * @author kalanihalemano
 */
public class CustomRecordReader implements RecordReader<LongWritable, Text>{

    private byte[] startTag;
    private byte[] endTag;
    private long start;
    private long end;
    private FSDataInputStream fsin;
    private final DataOutputBuffer buffer = new DataOutputBuffer();

    public CustomRecordReader(FileSplit split, JobConf jobConf) throws IOException{
        startTag="\n\n".getBytes();
        endTag="\n\n".getBytes();
        
        start=split.getStart();
        end=start + split.getLength();
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(jobConf);
        fsin = fs.open(file);
        fsin.seek(start);
    }
    
    @Override
    public boolean next(LongWritable k, Text v) throws IOException {
        if (fsin.getPos() < end){
            if (readUntilMatch(startTag, false)){
                try{
                    buffer.write(startTag);
                    if (readUntilMatch(endTag, true)){
                        k.set(fsin.getPos());
                        v.set(buffer.getData(),0,buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
        }
        return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return fsin.getPos();
    }

    @Override
    public void close() throws IOException {
        fsin.close();
    }

    @Override
    public float getProgress() throws IOException {
        return ((fsin.getPos() - start)/ (float)(end-start));
    }
    
    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException{
        int i=0;
        while (true){
            int b = fsin.read();
            
            if (b == -1) return false;
            
            if (withinBlock) buffer.write(b);
         
            if(b == match[i]){
                i++;
                if (i >= match.length) return true;
            } else i = 0;
            
            if (!withinBlock && i==0 && fsin.getPos() >= end) return false;
        }
    }
    
}
