package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.AdditionalAttrGroupingComparator;
import util.PrimaryKeyOnlyPartitioner;

public class ValueToKeyConversion
{

    public static class ValueToKeyMapper extends Mapper<Object, Text, Text, Text>
    {

        private Text primaryKey = new Text();
        private Text otherValues = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            int tokenCount = 0;
            String keyToEmit = "";
            String valuesToEmit = "";
            boolean firstTime=true;
            
            while (itr.hasMoreTokens())
            {
                String token = itr.nextToken();
                if (tokenCount == 0)
                {
                    keyToEmit = keyToEmit + token;
                }
                else if (tokenCount == 10)
                {
                    keyToEmit = keyToEmit + "-" + token;
                    valuesToEmit = valuesToEmit + ", "+ token ;
                }
                else
                {
                    if(firstTime)
                        valuesToEmit = valuesToEmit + token;
                    else
                        valuesToEmit = valuesToEmit + ", "+ token ;
                    firstTime=false;
                }
                tokenCount++;
            }
            primaryKey.set(keyToEmit);
            otherValues.set(valuesToEmit);
            context.write(primaryKey, otherValues);
        }
    }

    public static class ValueToKeyReducer extends Reducer<Text, Text, Text, Text>
    {
        Text newKey = new Text();
        Text finalValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String[] keyParts = key.toString().split("-");
            String keyPart = keyParts[0];
            newKey.set(keyPart);

            for (Text val : values)
            {
                context.write(newKey, val);

            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ValueToKey");
        job.setJarByClass(ValueToKeyConversion.class);

        job.setMapperClass(ValueToKeyMapper.class);
        job.setReducerClass(ValueToKeyReducer.class);
        job.setPartitionerClass(PrimaryKeyOnlyPartitioner.class);
        job.setSortComparatorClass(AdditionalAttrGroupingComparator.class);
        job.setGroupingComparatorClass(AdditionalAttrGroupingComparator.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
