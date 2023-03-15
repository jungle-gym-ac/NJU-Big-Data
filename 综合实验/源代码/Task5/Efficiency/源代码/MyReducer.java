import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;


public class MyReducer extends Reducer<Text,Text,Text, Text>{
    private HashMap<String, Integer> records = new HashMap<String, Integer>();

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //依次读取Distributed Cache中每一场比赛结果
        try{
            Path path=new Path(context.getCacheFiles()[0].getPath());
            FileSystem fs=FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            String line,winTeam,loseTeam;
            String[] tokens;
            BufferedReader joinReader=new BufferedReader(
                                        new InputStreamReader(fis, "UTF-8"));
            try {
                while ((line = joinReader.readLine()) != null) {
                    tokens = line.split(",", 5);
                    if(Integer.parseInt(tokens[2])>Integer.parseInt(tokens[4])){
                        winTeam=tokens[1]+"Win";
                        loseTeam=tokens[3]+"Lose";
                    }
                    else{
                        winTeam=tokens[3]+"Win";
                        loseTeam=tokens[1]+"Lose";
                    }
                    if(records.containsKey(winTeam)){
                        records.put(winTeam, records.get(winTeam)+1);
                    }
                    else{
                        records.put(winTeam, 1);
                    }
                    if(records.containsKey(loseTeam)){
                        records.put(loseTeam,records.get(loseTeam)+1);
                    }
                    else{
                        records.put(loseTeam,1);
                    }
                }
            } finally {
                joinReader.close();
            }
        }
        catch (IOException e)
        {
            System.err.println("Exception reading :"+e);
        }
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException{
        String[] tokens=key.toString().split("#",2);
        String team=tokens[0];
        
        int score=0;

        for(Text value:values){
            score+=Integer.parseInt(value.toString());
        }
        int win=records.containsKey(team+"Win")?records.get(team+"Win"):0;
        int lose=records.containsKey(team+"Lose")?records.get(team+"Lose"):0;
        Double efficiency=new Double(score)/(win+lose);

        context.write(new Text(Double.toString(efficiency)),key);
    }
}
