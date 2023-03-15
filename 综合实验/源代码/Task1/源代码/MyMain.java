import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyMain {

    public static void main(String[] args) throws Exception{
        //args: 命令行参数，分别为:输入数据集路径，输出路径
      
                Configuration conf=new Configuration();

                conf.set("mapred.textoutputformat.separator", ","); //设置输出中Key和Value的分隔符，不设置则默认为\t
                
                Job job = Job.getInstance(conf,"GameResult");

                job.setJarByClass(MyMain.class);
                job.setMapperClass(MyMapper.class);
                job.setReducerClass(MyReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));//输入数据集的路径
                FileOutputFormat.setOutputPath(job,new Path(args[1]));//输出路径

                job.waitForCompletion(true); //执行MapReduce Job，等待结束
        
    }
}
