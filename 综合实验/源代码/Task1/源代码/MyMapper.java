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
        int point=0; //得分
        
        if(event.get("FreeThrowOutcome").equals("make")){ //罚球得分
            point=1;
        }
        else if(event.get("ShotOutcome").equals("make")){ //投篮得分
            point= (event.get("ShotType").equals("2-pt")) ? 2:3 ; //两分球得2分，三分球得三分
        }
        
        if(point==0) return; //没有得分

        //Emit
        String outputKey=String.join("#",event.get("Date"),event.get("HomeTeam"),event.get("AwayTeam"));
        String outputValue=String.join("#",event.get("PlayBy"),Integer.toString(point));
        context.write(new Text(outputKey),new Text(outputValue));
    }
}
