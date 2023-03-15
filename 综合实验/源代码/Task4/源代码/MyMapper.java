import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, Text,Text> {


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        if(key.toString().equals("0")){ //文件第一行,不处理
            return;
        }

        Event event=new Event(value);
        String outputKey=event.get("PlayBy");
        int score=0;
        
        
        
        if(event.get("ShotOutcome").equals("make")){ //投篮得分
            outputKey=String.join("#",outputKey,event.get("Shooter"));
            score= (event.get("ShotType").equals("2-pt")) ? 2:3 ; //两分球得2分，三分球得三分

            //助攻
            if(event.get("Assister")!=null){
                context.write(new Text(outputKey),new Text(Integer.toString(score)));
                outputKey=String.join("#"
                    ,event.get("PlayBy")
                    ,event.get("Assister"));
                score=1;
            }
        }
        else if(event.get("ShotOutcome").equals("miss")){ //错失投篮
            outputKey=String.join("#",outputKey,event.get("Shooter"));
            score=-1;
            //盖帽
            if(event.get("Blocker")!=null){
                context.write(new Text(outputKey),new Text(Integer.toString(score)));
                outputKey=String.join("#"
                    ,event.get("PlayBy").equals(event.get("HomeTeam")) ? event.get("AwayTeam") : event.get("HomeTeam")
                    ,event.get("Blocker"));
                score=1;
            }
        }
        else if (event.get("Rebounder")!=null && !event.get("Rebounder").equals("Team")){ //篮板
            outputKey=String.join("#",outputKey,event.get("Rebounder"));
            score=1;
        }
        else if(event.get("FreeThrowOutcome").equals("make")){ //罚球得分
            outputKey=String.join("#",outputKey,event.get("FreeThrowShooter"));
            score=1;
        }
        else if(event.get("FreeThrowOutcome").equals("miss")){ //错失罚球
            outputKey=String.join("#",outputKey,event.get("FreeThrowShooter"));
            score=-1;
        }
        else if (event.get("TurnoverPlayer")!=null && !event.get("TurnoverPlayer").equals("Team")){ //失误
            outputKey=String.join("#",outputKey,event.get("TurnoverPlayer"));
            score=-1;
            //抢断
            if(event.get("TurnoverCauser")!=null){
                context.write(new Text(outputKey),new Text(Integer.toString(score)));
                outputKey=String.join("#"
                    ,event.get("PlayBy").equals(event.get("HomeTeam")) ? event.get("AwayTeam") : event.get("HomeTeam")
                    ,event.get("TurnoverCauser"));
                score=1;
            }
        }

        if(score==0) return;

        //Emit
        context.write(new Text(outputKey),new Text(Integer.toString(score)));
    }
}
