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
            if (!((event.get("PlayBy").equals("team025")) || (event.get("PlayBy").equals("team028"))))//其他队伍
                continue;
            if(!(((event.get("Quarter").equals("4")) && (Integer.parseInt(event.get("SecLeft")) <= 300))
            || (Integer.parseInt(event.get("Quarter")) > 4))) //第四节最后5分钟+加时赛
                continue;
            if (!event.get("Shooter").equals("")) {
                String type = event.get("ShotType");
                String result = event.get("ShotOutcome");
                context.write(new Text(event.get("PlayBy") + "/" + event.get("Shooter")), new Text(type + "/" + result));
            }
        }
    }
}

