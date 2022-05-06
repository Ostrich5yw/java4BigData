package demo;

import bean.joinTestBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class reducejoinTestDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(reducejoinTestDriver.class);
        job.setMapperClass(joinTestMapper.class);
        job.setReducerClass(joinTestReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(joinTestBean.class);

        job.setOutputKeyClass(joinTestBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoop\\input\\joinTest"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output\\joinTest"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}

class joinTestMapper extends Mapper<LongWritable, Text, Text, joinTestBean>{
    String filename;
    joinTestBean outValue = new joinTestBean();
    Text outKey = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取对应文件名称
        InputSplit split = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) split;
        filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        if(filename.contains("order")){
            outKey.set(line[1]);                // key为产品id
            outValue.setId(line[0]);
            outValue.setPid(line[1]);
            outValue.setAmount(Integer.parseInt(line[2]));
            outValue.setPname("");
            outValue.setFlag("order");
        } else if (filename.contains("product")){
            outKey.set(line[0]);
            outValue.setId("");
            outValue.setPid(line[1]);
            outValue.setAmount(0);
            outValue.setPname(line[1]);
            outValue.setFlag("product");
        }
        context.write(outKey, outValue);
    }
}

class joinTestReducer extends Reducer<Text, joinTestBean, joinTestBean, NullWritable>{
    @Override                       // invalue中是相同产品id的所有数据
    protected void reduce(Text key, Iterable<joinTestBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<joinTestBean> orderList = new ArrayList<>();  // 记录id = x 的多个order内容
        joinTestBean jbBean = new joinTestBean();       // 记录id = x  name = xxx的product内容
        for(joinTestBean value : values){
            switch (value.getFlag()){
                case "order":
                    //创建一个临时joinTestBean对象接收value
                    joinTestBean tmpOrderBean = new joinTestBean();
                    try {
                        BeanUtils.copyProperties(tmpOrderBean,value);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    //将临时joinTestBean对象添加到集合orderList
                    orderList.add(tmpOrderBean);
                    break;
                case "product":
                    try {
                        BeanUtils.copyProperties(jbBean,value);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    break;
            }
        }
        for(joinTestBean tmp: orderList){
             tmp.setPname(jbBean.getPname());       // 将product中的Pname属性赋给orderList中的所有order对象（Flag == order）
             context.write(tmp, NullWritable.get());
        }
    }

    }
