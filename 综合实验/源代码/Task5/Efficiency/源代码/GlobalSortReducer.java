import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class GlobalSortReducer extends Reducer<Text, Text, Text, Text>{ 

    
    private MultipleOutputs<Text,Text> mos; //使用MultipleOutputs,将结果输出到多个文件

    @Override
    protected void setup(Context context){  //创建对象 
        mos = new MultipleOutputs<Text,Text>(context);
    }

    
    @Override
     protected void cleanup(Context context) throws IOException,InterruptedException {  //关闭对象
        mos.close();  
    }  


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
                            throws IOException, InterruptedException{               
        String [] tokens;
        for(Text value:values){
            tokens=value.toString().split("#");
            mos.write(new Text(tokens[1]), key, tokens[0]);
        }
    } 
}

