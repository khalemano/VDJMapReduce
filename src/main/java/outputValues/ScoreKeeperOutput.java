/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package outputValues;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author kalanihalemano
 */
public class ScoreKeeperOutput implements Writable {
    
    private HashMap<String,Integer> data = new HashMap<>();

    public void addToCounter(String counter, int x){
        if (data.containsKey(counter)){
            data.put(counter, data.get(counter) + x);
        } else {
            data.put(counter, x);
        }
    }
    
    public Set<String> getKeys(){
        return data.keySet();
    }
    
    public int getValue(String key){
        return data.get(key);
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
       for (String key: data.keySet()){
           d.writeBoolean(true);
           d.writeUTF(key);
           d.writeInt(data.get(key));
       }
       d.writeBoolean(false);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        while(di.readBoolean()){
            data.put(di.readUTF(), di.readInt());
        }
    }
    
    @Override
    public String toString(){
        String result="";
        for(String key: data.keySet()){
            result = result + " " +
                    key + ":" + data.get(key);
        }
        
        return result;
    }
    
}
