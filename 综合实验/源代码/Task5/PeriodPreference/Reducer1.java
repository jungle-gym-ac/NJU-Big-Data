import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        Text name = new Text(key);
        Set<String> Periods = new HashSet<>();
        HashMap<String, Integer> PeriodMap = new HashMap<>();
        for (; (it.hasNext());) {
            String item = it.next().toString();
            Periods.add(item);
        }
        for (String pd : Periods) {
            String period = pd.split(":")[1];
            if (PeriodMap.containsKey(period)) {
                int cur = PeriodMap.get(period);
                PeriodMap.put(period, cur + 1);
            }
            else
                PeriodMap.put(period, 1);
        }
        for (String k : PeriodMap.keySet()) {
            context.write(name, new Text(k + "/" + PeriodMap.get(k)));//name + team, period + times
        }
    }
}
