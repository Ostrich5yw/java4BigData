package partition;

import bean.compareFlowBean;
import bean.flowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class partitionWithCompare extends Partitioner<compareFlowBean, Text> {
    @Override
    public int getPartition(compareFlowBean flowBean, Text text, int numPartitions) {
        String prephone = text.toString().substring(0, 3);
        switch (prephone){
            case "136":
                return 1;
            case "137":
                return 2;
            case "138":
                return 3;
            case "139":
                return 4;
            default:
                return 0;
        }
    }
}
