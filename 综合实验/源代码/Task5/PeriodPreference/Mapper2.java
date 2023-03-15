import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");//读取行
        while (itr.hasMoreTokens()) {
            String line = itr.nextToken();
            String name = line.split(";")[0];
            String team = name.split("/")[0];
            name = name.split("/")[1];
            String period = line.split(";")[1];
            String times = period.split("/")[1];
            period = period.split("/")[0];
            context.write(new Text(team + "/" + period),
                    new Text(name + "/" + times));//队伍+节，名字+次数
        }
    }
}