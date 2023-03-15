import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        Text name = new Text(key);
        int Assist = 0, Block = 0, TurnOverCause = 0, Rebound = 0, Score = 0;
        for(;it.hasNext();) {
            String type = it.next().toString();
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
        context.write(name, new Text("Score:" + Score));
        context.write(name, new Text("Rebound:" + Rebound));
        context.write(name, new Text("Assist:" + Assist));
        context.write(name, new Text("TurnOverCause:" + TurnOverCause));
        context.write(name, new Text("Block:" + Block));
    }
}