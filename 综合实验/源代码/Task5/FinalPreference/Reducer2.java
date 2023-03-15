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
    private class PersonalShoot {
        Integer success;
        Integer total;
        PersonalShoot(){}
        PersonalShoot(int success, int total) {
            this.success = success;
            this.total = total;
        }
        public int getSuccess() {return this.success;}
        public int getTotal() {return this.total;}
    }
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        String team_type = key.toString();
        HashMap<String, PersonalShoot> ShootMap = new HashMap<>();
        int all_success = 0, all_total = 0;
        for (; (it.hasNext());) {
            String line = it.next().toString();
            String name = line.split("/", 3)[0];
            int success = Integer.parseInt(line.split("/", 3)[1]);
            int total = Integer.parseInt(line.split("/", 3)[2]);
            all_success += success;
            all_total += total;
            PersonalShoot ps = new PersonalShoot(success, total);
            ShootMap.put(name, ps);
        }
        List<Map.Entry<String,PersonalShoot>> list = new ArrayList<>(ShootMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, PersonalShoot>>() {
            @Override
            public int compare(Entry<String, PersonalShoot> o1, Entry<String, PersonalShoot> o2) {
                if (o2.getValue().getSuccess() > o1.getValue().getSuccess())
                    return 1;
                else if (o2.getValue().getSuccess() < o1.getValue().getSuccess())
                    return -1;
                else
                    return 0;
            }
        });
        String output = new String("All Players: make " + all_success + ", total " + all_total + "; ");
        for(Map.Entry<String,PersonalShoot> mapping:list){
            output = output.concat(mapping.getKey());
            output = output.concat(": make ");
            output = output.concat(Integer.toString(mapping.getValue().getSuccess()));
            output = output.concat(", total ");
            output = output.concat(Integer.toString(mapping.getValue().getTotal()));
            output = output.concat("; ");
        }
        context.write(new Text(team_type), new Text(output));
    }
}