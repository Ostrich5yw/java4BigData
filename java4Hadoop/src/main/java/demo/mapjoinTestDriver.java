package demo;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class mapjoinTestDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(mapjoinTestDriver.class);
    job.setMapperClass(mapjoinTestMapper.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    // 加载缓存数据
    job.addCacheFile(new URI("file:///D:/hadoop/input/joinTest/product.txt"));
    // Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path("D:\\hadoop\\input\\joinTest\\order.txt"));
    FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output\\mapjoinTest"));

    boolean b = job.waitForCompletion(true);
    System.exit(b ? 0 : 1);

}
}

class mapjoinTestMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> pdMap = new HashMap<>();
    private Text text = new Text();

    //任务开始前将pd数据缓存进pdMap
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //通过缓存文件得到小表数据pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //获取文件系统对象,并开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        //通过包装流转换为reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //切割一行
            //01	小米
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }

        //关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //读取大表数据
        //1001	01	1
        String[] fields = value.toString().split("\t");

        //通过大表每行数据的pid,去pdMap里面取出pname
        String pname = pdMap.get(fields[1]);

        //将大表每行数据的pid替换为pname
        text.set(fields[0] + "\t" + pname + "\t" + fields[2]);

        //写出
        context.write(text, NullWritable.get());
    }
}
