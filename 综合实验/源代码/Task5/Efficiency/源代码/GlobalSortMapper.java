import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GlobalSortMapper extends Mapper<Text, Text, Text, Text>{
    @Override
    protected void map(Text key, Text value, Context context)
                        throws IOException, InterruptedException{ 
        context.write(key,value);
    }
}
