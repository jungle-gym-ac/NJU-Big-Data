import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");//读取行
        while (itr.hasMoreTokens()) {
            String line = itr.nextToken();
            String []Values = line.split("/", 5);
            String team = Values[0];
            String name = Values[1];
            String type = Values[2];
            String success = Values[3];
            String total = Values[4];
            context.write(new Text(team + "/" + type),
                    new Text(name + "/" + success + "/" + total));//队伍+得分类型，名字+次数
        }
    }
}