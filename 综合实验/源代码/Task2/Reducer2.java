import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class Reducer2 extends Reducer<TypeScore, Text, Text, Text> {
    Integer i = 0;
    String cur_type = new String();
    public void reduce(TypeScore key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        int score = key.getScore();
        String type = key.getType();
        if((cur_type.equals("")) || (!cur_type.equals(type))) {//this type finishes
            cur_type = type;
            i = 0;
        }
        for (; (it.hasNext()) && (i < 5); i++) {//output the top 5 players
            String name = it.next().toString();
            context.write(new Text(type), new Text(name + ": " + score));
        }
    }
}
