package util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AdditionalAttrGroupingComparator extends WritableComparator {
    protected AdditionalAttrGroupingComparator() {
        super(Text.class, true);
        }   
    
    
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text k1 = (Text)w1;
        Text k2 = (Text)w2;

        String key1 =w1.toString();
        String key2 =w2.toString();

        int result = key1.split("-")[1].compareTo(key2.split("-")[1]);
        if(result==0)
        {
            String firstPartOfKey1 = key1.split("-")[0];
            String firstPartOfKey2 = key2.split("-")[0];
            result = Integer.valueOf(firstPartOfKey1).compareTo(Integer.valueOf(firstPartOfKey2)) ;
        }
        return result;

    }
   

}
