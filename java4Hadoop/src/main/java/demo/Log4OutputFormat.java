package demo;

import Writer.LogRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Log4OutputFormat{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Log4OutputFormat.class);
        job.setMapperClass(Log4OutputFormatMapper.class);
        job.setReducerClass(Log4OutputFormatReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置自定义的outputformat
        job.setOutputFormatClass(Log4OutputFormatWriter.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoop\\input\\4log.txt"));
        //虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        //而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output\\Log4OutputFormat"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

class Log4OutputFormatMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    LongWritable outVal = new LongWritable();
    Text outKey = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split(" ");
        outVal.set(Long.parseLong(val[0]));
        outKey.set(val[1]);
        context.write(outKey, outVal);
    }
}

class Log4OutputFormatReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    LongWritable outVal = new LongWritable();
    long sum = 0;
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;
        for(LongWritable value: values) {
           sum += value.get();
        }
        outVal.set(sum);
        context.write(key, outVal);
    }
}

class Log4OutputFormatWriter extends FileOutputFormat<Text, LongWritable> {
    @Override
    public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        //创建一个自定义的RecordWriter返回
        LogRecordWriter logRecordWriter = new LogRecordWriter(job);
        return logRecordWriter;
    }
}
