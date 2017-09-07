package wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperCombiner
{

    public static class InmapperTokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {

        private Text word = new Text();
        Map<String, Integer> inmemMap = null;

        protected void setup(Context context) throws IOException, InterruptedException
        {
            inmemMap = new HashMap<String, Integer>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString(), " ,");
            while (itr.hasMoreTokens())
            {
                String token = itr.nextToken();
                if (!inmemMap.containsKey(token))
                {
                    inmemMap.put(token, 1);
                }
                else
                {
                    int currentValue = inmemMap.get(token);
                    inmemMap.put(token, currentValue + 1);

                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            for (String key : inmemMap.keySet())
            {
                word.set(key);

                context.write(word, new IntWritable(inmemMap.get(key)));
            }
        }

    }

    public static class InmapperIntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(InMapperCombiner.class);

        job.setMapperClass(InmapperTokenizerMapper.class);
        job.setReducerClass(InmapperIntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
