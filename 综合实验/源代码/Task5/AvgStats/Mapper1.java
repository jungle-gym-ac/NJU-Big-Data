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
            String team1 = event.get("AwayTeam");
            String team2 = event.get("HomeTeam");
            String playby = event.get("PlayBy");
            String date = event.get("Date");
            if (!(team1.equals("team025") || team1.equals("team028") ||
                    team2.equals("team025") || team2.equals("team028")))//其他队伍
                continue;
            if (!event.get("Assister").equals("") && (playby.equals("team025") || playby.equals("team028"))) {
                context.write(new Text(event.get("Assister") + "/" + playby), new Text("Assist" + ":" + date));
            }
            if (!event.get("Blocker").equals("")) {
                if (playby.equals(team1) && (team2.equals("team025") || team2.equals("team028")))
                    context.write(new Text(event.get("Blocker") + "/" + team2), new Text("Block" + ":" + date));
                else if (playby.equals(team2) && (team1.equals("team025") || team1.equals("team028")))
                    context.write(new Text(event.get("Blocker") + "/" + team1), new Text("Block" + ":" + date));
            }
            if (!event.get("Rebounder").equals("") && (playby.equals("team025") || playby.equals("team028"))) {
                if (!event.get("Rebounder").equals("Team"))
                    context.write(new Text(event.get("Rebounder") + "/" + playby), new Text("Rebound" + ":" + date));
            }
            if (!event.get("TurnoverCauser").equals("")) {
                if (playby.equals(team1) && (team2.equals("team025") || team2.equals("team028")))
                    context.write(new Text(event.get("TurnoverCauser") + "/" + team2), new Text("TurnOverCause" + ":" + date));
                else if (playby.equals(team2) && (team1.equals("team025") || team1.equals("team028")))
                    context.write(new Text(event.get("TurnoverCauser") + "/" + team1), new Text("TurnOverCause" + ":" + date));
            }
            if (event.get("ShotOutcome").equals("make") && (playby.equals("team025") || playby.equals("team028"))) {//score
                String score = event.get("ShotType").split("-")[0];
                context.write(new Text(event.get("Shooter") + "/" + playby), new Text(score + ":" + date));
            }
            if (event.get("FreeThrowOutcome").equals("make") && (playby.equals("team025") || playby.equals("team028"))) {//score
                context.write(new Text(event.get("FreeThrowShooter") + "/" + playby), new Text("1" + ":" + date));
            }
        }
    }
}