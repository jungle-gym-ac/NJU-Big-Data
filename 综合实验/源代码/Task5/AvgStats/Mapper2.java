import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, Text, TypeScore> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");//读取行
        while (itr.hasMoreTokens()) {
            String line = itr.nextToken();
            String name = line.split(";")[0];
            String team = name.split("/")[1];
            name = name.split("/")[0];
            line = line.split(";")[1];
            String type = line.split(":")[0];
            double score = Double.parseDouble(line.split(":")[1]);
            if (type.equals("Assist")) {
                context.write(new Text(team), new TypeScore("Assist", score, name));
            }
            else if (type.equals("TurnOverCause")) {
                context.write(new Text(team), new TypeScore("TurnOverCause", score, name));
            }
            else if (type.equals("Block")) {
                context.write(new Text(team), new TypeScore("Block", score, name));
            }
            else if (type.equals("Rebound")) {
                context.write(new Text(team), new TypeScore("Rebound", score, name));
            }
            else if (type.equals("Score")) {
                context.write(new Text(team), new TypeScore("Score", score, name));
            }
        }
    }
}