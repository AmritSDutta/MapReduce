package util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PrimaryKeyOnlyPartitioner extends Partitioner<Text, Text> {
    
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {

        String [] keyParts = key.toString().split("-");
        String keyPart = keyParts[1];
       
        //this is done to avoid performing mod with 0
        if(numReduceTasks == 0)
            return 0;
        else
            return keyPart.hashCode() % numReduceTasks;
       
    }
}