import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class Reducer2 extends Reducer<Text, TypeScore, Text, Text> {
    public void reduce(Text key, Iterable<TypeScore> values, Context context) throws IOException, InterruptedException {
        Iterator<TypeScore> it = values.iterator();
        HashMap<String, TypeScore> ScoreMap = new HashMap<>();
        String team = key.toString();
        for (; (it.hasNext());) {
            TypeScore tp = it.next();
            String type = tp.getType();
            String name = tp.getName();
            Double score = tp.getScore();
            ScoreMap.put(name + ":" + type, new TypeScore(type, score, team));
            }
        List<Map.Entry<String,TypeScore>> list = new ArrayList<Map.Entry<String,TypeScore>>(ScoreMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, TypeScore>>() {
            @Override
            public int compare(Map.Entry<String, TypeScore> o1, Map.Entry<String, TypeScore> o2) {
                if(o2.getValue().getType().compareTo(o1.getValue().getType()) != 0)
                    return o2.getValue().getType().compareTo(o1.getValue().getType()); // 按type升排序
                else if (o2.getValue().getScore() > o1.getValue().getScore())
                    return 1;// 按value降序排序
                else if (o2.getValue().getScore() < o1.getValue().getScore())
                    return -1;
                else
                    return 0;
            }
        });
        String output = new String();
        String cur_type = new String();
        for(Map.Entry<String,TypeScore> mapping:list){
            if(cur_type.equals(""))
                cur_type = mapping.getValue().getType();
            else if (!cur_type.equals(mapping.getValue().getType())) {
                context.write(new Text(team + "\t" + cur_type), new Text(output));
                cur_type = mapping.getValue().getType();
                output = "";
            }
            output = output.concat(mapping.getKey().split(":")[0]);
            output = output.concat(":");
            output = output.concat(String.format("%.5f", mapping.getValue().getScore()));
            output = output.concat(", ");
        }
        context.write(new Text(team + "\t" + cur_type), new Text(output));
        //context.write(new Text(team ),
        //        new Text(mapping.getValue().getType() + "\t" + mapping.getKey().split(":")[0] + "\t" +
        //                String.format("%.5f", mapping.getValue().getScore())));
    }
}