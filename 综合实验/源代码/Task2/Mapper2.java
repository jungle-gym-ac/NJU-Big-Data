import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, TypeScore, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");//读取行
        while (itr.hasMoreTokens()) {
            String line = itr.nextToken();
            String name = line.split(";")[0];
            line = line.split(";")[1];
            String type = line.split(":")[0];
            Integer score = Integer.parseInt(line.split(":")[1]);
            if (type.equals("Assist")) {
                context.write(new TypeScore("Assist", score), new Text(name));
            }
            else if (type.equals("TurnOverCause")) {
                context.write(new TypeScore("TurnOverCause", score), new Text(name));
            }
            else if (type.equals("Block")) {
                context.write(new TypeScore("Block", score), new Text(name));
            }
            else if (type.equals("Rebound")) {
                context.write(new TypeScore("Rebound", score), new Text(name));
            }
            else if (type.equals("Score")) {
                context.write(new TypeScore("Score", score), new Text(name));
            }
        }
    }
}
