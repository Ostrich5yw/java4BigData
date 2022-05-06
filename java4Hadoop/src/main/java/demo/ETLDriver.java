package demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ETLDriver {
    public static void main(String[] args) throws Exception {
        args = new String[] { "D:/hadoop/input/4ETL.txt", "D:/hadoop/output/ETL" };

        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(ETLDriver.class);

        // 3 关联map
        job.setMapperClass(ETLMapper.class);

        // 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reducetask个数为0
        job.setNumReduceTasks(0);

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}

class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取1行数据
        String line = value.toString();
        // 2 解析日志
        boolean result = parseLog(line,context);
        // 3 日志不合法退出
        if (!result) {
            return;
        }
        // 4 日志合法就直接写出
        context.write(value, NullWritable.get());
    }

    // 2 封装解析日志的方法
    private boolean parseLog(String line, Context context) {
        // 1 截取
        String[] fields = line.split(" ");
        // 2 日志长度大于11的为合法
        if (fields.length > 11) {
            return true;
        }else {
            return false;
        }
    }
}

