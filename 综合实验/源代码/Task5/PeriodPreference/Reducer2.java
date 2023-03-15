import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;
import java.util.*;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        String cur_period = key.toString();
        HashMap<String, Integer> PeriodMap = new HashMap<>();
        for (; (it.hasNext());) {
            String line = it.next().toString();
            String name = line.toString().split("/")[0];
            int times = Integer.parseInt(line.split("/")[1]);
            PeriodMap.put(name, times);
        }
        List<Map.Entry<String,Integer>> list = new ArrayList<Map.Entry<String,Integer>>(PeriodMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        String output = new String();
        for(Map.Entry<String,Integer> mapping:list){
            output = output.concat(mapping.getKey());
            output = output.concat(":");
            output = output.concat(Integer.toString(mapping.getValue()));
            output = output.concat(", ");
        }
        context.write(new Text(cur_period), new Text(output));
    }
}
