import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");//读取行
        while (itr.hasMoreTokens()) {
            Event event = new Event(itr.nextToken());
            if (event.get("Assister").equals("Assister"))//表头
                continue;
            if (!event.get("Assister").equals("")) {
                context.write(new Text(event.get("Assister")), new Text("Assist"));
            }
            if (!event.get("Blocker").equals("")) {
                context.write(new Text(event.get("Blocker")), new Text("Block"));
            }
            if (!event.get("Rebounder").equals("")) {
                if (event.get("Rebounder").equals("Team"))
                    continue;
                context.write(new Text(event.get("Rebounder")), new Text("Rebound"));
            }
            if (!event.get("TurnoverCauser").equals("")) {
                context.write(new Text(event.get("TurnoverCauser")), new Text("TurnOverCause"));
            }
            if (event.get("ShotOutcome").equals("make")) {//score
                String score = event.get("ShotType").split("-")[0];
                context.write(new Text(event.get("Shooter")), new Text(score));
            }
            if (event.get("FreeThrowOutcome").equals("make")) {//score
                context.write(new Text(event.get("FreeThrowShooter")), new Text("1"));
            }
        }
    }
}
