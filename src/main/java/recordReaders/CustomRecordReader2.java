package recordReaders;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CustomRecordReader2 extends RecordReader<LongWritable, Text> {

    private long start;
    private long end;

    private FSDataInputStream fsin;
    private final DataOutputBuffer buffer = new DataOutputBuffer();

    private byte[] delimiter;

    private LongWritable key = new LongWritable();
    private Text value = new Text();

    private boolean firstRecordOfSplit;
    private boolean startOfFile;
    
    Path file;

    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        delimiter = "\n\n".getBytes();

        FileSplit split = (FileSplit) is;
        Configuration job = tac.getConfiguration();

        start = split.getStart();
        startOfFile = start == 0;
        firstRecordOfSplit = true;

        end = start + split.getLength();
        file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        fsin = fs.open(file);
        fsin.seek(start);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // check if past end of split
        if (fsin.getPos() >= end) {
            return false;
        }
        // check if first record of split
        if (firstRecordOfSplit) {
            firstRecordOfSplit = false;
            if (!startOfFile) {
                if (!readUntilMatch(delimiter, false)) {
                    return false;
                }
            }
        }
        // read until match, set the key and value
        if (readUntilMatch(delimiter, true)) {
            key.set(fsin.getPos());
            String result = new String (buffer.getData());
            value.set(result);
            buffer.reset();
            return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return ((fsin.getPos() - start) / (float) (end - start));
    }

    @Override
    public void close() throws IOException {
        fsin.close();
    }

    private boolean readUntilMatch(byte[] match, boolean writeBytes) throws IOException {
        int i = 0;
        while (true) {
            //read in the byte
            int b = fsin.read();
            // if it reaches the end of the file
            if (b == -1) {
                return true;
            }
            // 
            if (writeBytes) {
                buffer.write(b);
            }

            if (b == match[i]) {
                i++;
                if (i >= match.length) {
                    return true;
                }
            } else {
                i = 0;
            }
        }
    }
}
