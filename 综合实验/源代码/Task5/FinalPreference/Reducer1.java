import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        Text name = new Text(key);
        HashMap<String, Integer> SuccessMap = new HashMap<>();
        HashMap<String, Integer> TotalMap = new HashMap<>();
        for (; (it.hasNext());) {
            String item = it.next().toString();
            String type = item.split("/")[0];
            String result = item.split("/")[1];
            if (TotalMap.containsKey(type)) {
                int cur = TotalMap.get(type);
                TotalMap.put(type, cur + 1);
            }
            else
                TotalMap.put(type, 1);
            if (result.equals("make")) {
                if (SuccessMap.containsKey(type)) {
                    int cur = SuccessMap.get(type);
                    SuccessMap.put(type, cur + 1);
                }
                else
                    SuccessMap.put(type, 1);
            }
        }
        for (String k : TotalMap.keySet()) {
            if (SuccessMap.containsKey(k))
                context.write(name, new Text(k + "/" + SuccessMap.get(k) +
                        "/" + TotalMap.get(k)));//name + team, score-type + result
            else
                context.write(name, new Text(k + "/" + 0 +
                        "/" + TotalMap.get(k)));//name + team, score-type + result

        }
    }
}