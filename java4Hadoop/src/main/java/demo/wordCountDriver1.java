package demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class wordCountDriver1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 关联本Driver程序的jar
        job.setJarByClass(wordCountDriver1.class);
        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(wordCountMapper.class);
        job.setReducerClass(wordCountReducer.class);
        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, "D:\\hadoop\\input\\wordCount.txt");
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output\\wordCount"));
        // 7 设置InputFormat格式，默认为TextInputFormat
//        job.setInputFormatClass(CombineFileInputFormat.class);
//        CombineFileInputFormat.setMaxInputSplitSize(job, 4194304);  // 设置虚拟存储切片大小4M
        // 8. 设置combiner对每个maptask进行处理
        job.setCombinerClass(wordCountCombiner.class);
        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
class wordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {   // 输入key 输入value 输出key 输出value
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override               // 输入key 输入value 结果输出context(输出key，输出value)
    protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();
        // 2 切割
        String[] words = line.split(" ");
        // 3 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}

class wordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int sum;
    IntWritable out = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;
        for(IntWritable value: values) {
            sum += value.get();
        }
        out.set(sum);
        context.write(key, out);
    }
}

class wordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //封装outKV
        outV.set(sum);
        //写出outKV
        context.write(key,outV);
    }
}
