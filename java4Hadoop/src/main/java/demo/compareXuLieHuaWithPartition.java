package demo;

import bean.compareFlowBean;
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
import partition.partition1;
import partition.partitionWithCompare;

import java.io.IOException;

public class compareXuLieHuaWithPartition {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 关联本Driver程序的jar
        job.setJarByClass(compareXuLieHuaWithPartition.class);
        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(compareXuLieHuaMapper.class);
        job.setReducerClass(compareXuLieHuaReducer.class);
        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(compareFlowBean.class);
        job.setMapOutputValueClass(Text.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(compareFlowBean.class);
        // 6 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, "D:\\hadoop\\input\\phone_data.txt");
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output\\compareXuLieHua"));
        //8 指定自定义分区器
        job.setPartitionerClass(partitionWithCompare.class);
        //9 同时指定相应数量的ReduceTask
        job.setNumReduceTasks(5);
        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
class compareXuLieHuaMapper extends Mapper<LongWritable, Text, compareFlowBean, Text> {
    compareFlowBean outkey = new compareFlowBean();
    Text outbean = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] inval = value.toString().split("\t");
        outbean.set(inval[1]);
        outkey.setUpFlow(Long.parseLong(inval[inval.length - 3]));
        outkey.setDownFlow(Long.parseLong(inval[inval.length - 2]));
        outkey.setSumFlow(outkey.getDownFlow() + outkey.getUpFlow());
        context.write(outkey, outbean);
    }
}

class compareXuLieHuaReducer extends Reducer<compareFlowBean, Text, Text, compareFlowBean> {
    @Override
    protected void reduce(compareFlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
