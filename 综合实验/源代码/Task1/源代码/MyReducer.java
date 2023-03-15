import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class MyReducer extends Reducer<Text,Text,Text, Text>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException{
        String[] tokens=key.toString().split("#",3);
        String date=tokens[0],homeTeam=tokens[1],awayTeam=tokens[2];
        int homeScore=0,awayScore=0; //主队和客队得分
        String playBy;//得分队伍
        int score; //得分
        for(Text value:values){
            playBy=value.toString().split("#")[0];
            score=Integer.parseInt(value.toString().split("#")[1]);
            if(playBy.equals(homeTeam)){ //主队得分
                homeScore+=score;
            }
            else{//客队得分
                awayScore+=score;
            }
        }
        String outputKey=date;
        String outputValue=String.join(",",homeTeam,Integer.toString(homeScore),awayTeam,Integer.toString(awayScore));
        context.write(new Text(outputKey), new Text(outputValue));
    }
}
