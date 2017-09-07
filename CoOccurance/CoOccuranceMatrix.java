package wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CoOccuranceMatrix
{

    public static class CoOccuranceMapper extends Mapper<Object, Text, Text, MapWritable>
    {

        //private Text word = new Text();
        Text word = null;
        Map<Text,MapWritable> coOccurMatrix=null;
        MapWritable inmemMap = null;

        protected void setup(Context context) throws IOException, InterruptedException
        {
            coOccurMatrix = new HashMap<Text,MapWritable>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString(), " ,.");
            int isFirst =0;
            while (itr.hasMoreTokens())
            {
                String token = itr.nextToken();
                Text tokenKey= new Text();
                if(isFirst == 0)
                {
                    word = new Text();
                    word.set(token);
                    isFirst++;
                }
                else {
                    tokenKey.set(token);

                    if (!coOccurMatrix.containsKey(word))
                    {
                        inmemMap = new MapWritable();
                        inmemMap.put(tokenKey, new IntWritable(1));
                        coOccurMatrix.put(word,inmemMap );
                    }
                    else
                    {
                        inmemMap = coOccurMatrix.get(word);
                        if (!inmemMap.containsKey(tokenKey))
                        {
                            inmemMap.put(tokenKey, new IntWritable(1));
                        }
                        else
                        {
                            IntWritable currentValue = (IntWritable) inmemMap.get(tokenKey);
                            currentValue.set(currentValue.get() + 1);
                            inmemMap.put(tokenKey,currentValue );
                        }
                        coOccurMatrix.put(word,inmemMap );
                    }
 
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            for (Text key : coOccurMatrix.keySet())
            {
                context.write(key, coOccurMatrix.get(key));
            }
        }

    }

    public static class CoOccuranceReducer extends Reducer<Text, MapWritable, Text, Text>
    {

        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException
        {
            MapWritable result = new MapWritable();
            for (MapWritable mapInstance : values)
            {
                for(Writable coOccuredWord :mapInstance.keySet())
                {
                    if(!result.containsKey(coOccuredWord))
                    {
                        result.put(coOccuredWord, mapInstance.get(coOccuredWord));
                    }
                    else
                    {
                       IntWritable oldCount =  (IntWritable) result.get(coOccuredWord);
                       IntWritable newCount=  (IntWritable) mapInstance.get(coOccuredWord);
                       int sum = oldCount.get() + newCount.get();
                       result.put(coOccuredWord, new IntWritable(sum));
                    }
                }
            }
            
            String allCoWordFreq="[";
            Set<Writable> coWords = result.keySet();
            for (Writable coWord : coWords)
            {
                allCoWordFreq =allCoWordFreq+ coWord;
                allCoWordFreq =allCoWordFreq+ "=";
                allCoWordFreq =allCoWordFreq+ result.get(coWord).toString();
                allCoWordFreq =allCoWordFreq+ "  ";

            }
            allCoWordFreq =allCoWordFreq+ " ]";
            context.write(key, new Text(allCoWordFreq));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Co Occurance Matrix");
        job.setJarByClass(CoOccuranceMatrix.class);

        job.setMapperClass(CoOccuranceMapper.class);
        job.setReducerClass(CoOccuranceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
