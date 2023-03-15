import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyPartitioner extends HashPartitioner<TypeScore, Text> { //按照类别来做partition
    @Override
    public int getPartition(TypeScore key, Text value, int numReduceTasks) {
        String type = key.getType();
        int num = 0;
        switch (type) {
            case "Assist":
                num = numReduceTasks - 1;
                break;
            case "TurnOverCause":
                num = numReduceTasks - 2;
                break;
            case "Block":
                num = numReduceTasks - 3;
                break;
            case "Rebound":
                num = numReduceTasks - 4;
                break;
            case "Score":
                num = numReduceTasks - 5;
                break;
        }
        return num;
    }
}