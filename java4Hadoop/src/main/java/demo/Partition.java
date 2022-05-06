package demo;

import bean.flowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import partition.partition1;

import java.io.IOException;

public class Partition {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 关联本Driver程序的jar
        job.setJarByClass(Partition.class);
        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(partitionMapper.class);
        job.setReducerClass(partitionReducer.class);
        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(flowBean.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(flowBean.class);
        // 6 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //8 指定自定义分区器
        job.setPartitionerClass(partition1.class);
        //9 同时指定相应数量的ReduceTask
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, "D:\\hadoop\\input\\phone_data.txt");
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output\\partition"));
        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

class partitionMapper extends Mapper<LongWritable, Text, Text, flowBean> {
    Text outkey = new Text();
    flowBean outbean = new flowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] inval = value.toString().split("\t");
        outkey.set(inval[1]);
        outbean.setUpFlow(Long.parseLong(inval[inval.length - 3]));
        outbean.setDownFlow(Long.parseLong(inval[inval.length - 2]));
        context.write(outkey, outbean);
    }
}

class partitionReducer extends Reducer<Text, flowBean, Text, flowBean> {
    long upSum, downSum;
    flowBean out = new flowBean();
    @Override
    protected void reduce(Text key, Iterable<flowBean> values, Context context) throws IOException, InterruptedException {
        upSum = 0;downSum = 0;
        for (flowBean value : values) {
            upSum = value.getUpFlow();
            downSum = value.getDownFlow();
        }
        out.setSumFlow(upSum + downSum);
        out.setDownFlow(downSum);
        out.setUpFlow(upSum);
        context.write(key, out);
    }
}