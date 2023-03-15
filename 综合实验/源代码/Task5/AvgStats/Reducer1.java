import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        Text name = new Text(key);
        double Assist = 0, Block = 0, TurnOverCause = 0, Rebound = 0, Score = 0;
        Set<String> Dates = new HashSet<>();
        for(;it.hasNext();) {
            String line = it.next().toString();
            String type = line.split(":")[0];
            String date = line.split(":")[1];
            Dates.add(date);
            if (type.equals("Assist")) {
                Assist += 1;
            }
            else if (type.equals("TurnOverCause")) {
                TurnOverCause += 1;
            }
            else if (type.equals("Block")) {
                Block += 1;
            }
            else if (type.equals("Rebound")) {
                Rebound += 1;
            }
            else {//Score
                Score += Integer.parseInt(type);
            }
        }
        int MatchNum = Dates.size();
        if (Score > 0)
            context.write(name, new Text("Score:" + (Score / MatchNum)));
        if (Rebound > 0)
            context.write(name, new Text("Rebound:" + (Rebound / MatchNum)));
        if (Assist > 0)
            context.write(name, new Text("Assist:" + (Assist / MatchNum)));
        if (TurnOverCause > 0)
            context.write(name, new Text("TurnOverCause:" + (TurnOverCause / MatchNum)));
        if (Block > 0)
            context.write(name, new Text("Block:" + (Block / MatchNum)));
    }
}