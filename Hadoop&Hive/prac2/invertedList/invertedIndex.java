import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class invertedIndex
{
    public static class Map
        extends Mapper<LongWritable, Text, Text, Text>
        {
            private Text documentId;
            private Text word = new Text();

            @Override
            protected void setup(Context context)
            {
                String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
                documentId = new Text(filename);
            }

            @Override
            protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
            {
                for(String token : StringUtils.split(value.toString()))
                {
                    word.set(token);
                    context.write(word, documentId);
                }
            }
        }

    public static class Reduce
        extends Reducer<Text, Text, Text, Text>
        {
            private Text docIds = new Text();
            public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
            {
                HashSet<Text> uniqueDocIds = new HashSet<Text>();
                for(Text docId : values)
                {
                    uniqueDocIds.add(docId);
                }
                docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));
                context.write(key, docIds);
            }
        }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(invertedIndex.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
