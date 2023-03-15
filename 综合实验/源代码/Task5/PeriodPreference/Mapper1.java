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
            String quarter = event.get("Quarter");

            if (!(team1.equals("team025") || team1.equals("team028") ||
                    team2.equals("team025") || team2.equals("team028")))//其他队伍
                continue;
            if (!event.get("Assister").equals("") && (playby.equals("team025") || playby.equals("team028"))) {
                context.write(new Text(playby + "/" + event.get("Assister")), new Text(date + ":" + quarter));
            }
            if (!event.get("Blocker").equals("")) {
                if (playby.equals(team1) && (team2.equals("team025") || team2.equals("team028")))
                    context.write(new Text(team2 + "/" + event.get("Blocker")),new Text(date + ":" + quarter));
                else if (playby.equals(team2) && (team1.equals("team025") || team1.equals("team028")))
                    context.write(new Text(team1 + "/" + event.get("Blocker")),new Text(date + ":" + quarter));
            }
            if (!event.get("Rebounder").equals("") && (playby.equals("team025") || playby.equals("team028"))) {
                if (!event.get("Rebounder").equals("Team"))
                    context.write(new Text(playby + "/" + event.get("Rebounder")), new Text(date + ":" + quarter));
            }
            if (!event.get("Shooter").equals("") && (playby.equals("team025") || playby.equals("team028"))) {
                context.write(new Text(playby + "/" + event.get("Shooter")), new Text(date + ":" + quarter));
            }
            if (!event.get("TurnoverPlayer").equals("") && (playby.equals("team025") || playby.equals("team028"))) {
                if (!event.get("TurnoverPlayer").equals("Team"))
                    context.write(new Text(playby + "/" + event.get("TurnoverPlayer")), new Text(date + ":" + quarter));
            }
            if (!event.get("TurnoverCauser").equals("")) {
                if (playby.equals(team1) && (team2.equals("team025") || team2.equals("team028")))
                    context.write(new Text(team2 + "/" + event.get("TurnoverCauser")), new Text(date + ":" + quarter));
                else if (playby.equals(team2) && (team1.equals("team025") || team1.equals("team028")))
                    context.write(new Text(team1 + "/" + event.get("TurnoverCauser")), new Text(date + ":" + quarter));
            }
            if (!event.get("Fouler").equals("") && !event.get("Fouler").equals("Team")) {
                if ((event.get("FoulType").equals("offensive") || event.get("FoulType").equals("loose ball")
                        || event.get("FoulType").equals("technical"))) {
                     if (playby.equals("team025") || playby.equals("team028"))
                         context.write(new Text(playby + "/" + event.get("Fouler")), new Text(date + ":" + quarter));
                }
                else {
                    if (playby.equals(team1) && (team2.equals("team025") || team2.equals("team028")))
                        context.write(new Text(team2 + "/" + event.get("Fouler")), new Text(date + ":" + quarter));
                    else if (playby.equals(team2) && (team1.equals("team025") || team1.equals("team028")))
                        context.write(new Text(team1 + "/" + event.get("Fouler")), new Text(date + ":" + quarter));
                }
            }
            if (!event.get("FreeThrowShooter").equals("") && (playby.equals("team025") || playby.equals("team028"))) {
                context.write(new Text(playby + "/" + event.get("FreeThrowShooter")), new Text(date + ":" + quarter));
            }
            if (!event.get("EnterGame").equals("") && (playby.equals("team025") || playby.equals("team028"))) {
                context.write(new Text(playby + "/" + event.get("EnterGame")), new Text(date + ":" + quarter));
            }
            if (!event.get("LeaveGame").equals("") && (playby.equals("team025") || playby.equals("team028"))) {
                context.write(new Text(playby + "/" + event.get("LeaveGame")), new Text(date + ":" + quarter));
            }
        }
    }
}