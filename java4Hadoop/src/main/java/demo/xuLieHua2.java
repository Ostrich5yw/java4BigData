package demo;

import bean.flowBean;
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

public class xuLieHua2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 关联本Driver程序的jar
        job.setJarByClass(xuLieHua2.class);
        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(xuLieHuaMapper.class);
        job.setReducerClass(xuLieHuaReducer.class);
        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(flowBean.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(flowBean.class);
        // 6 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, "D:\\hadoop\\input\\phone_data.txt");
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output\\xuLieHua"));
        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
class xuLieHuaMapper extends Mapper<LongWritable, Text, Text, flowBean> {
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

class xuLieHuaReducer extends Reducer<Text, flowBean, Text, flowBean> {
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
