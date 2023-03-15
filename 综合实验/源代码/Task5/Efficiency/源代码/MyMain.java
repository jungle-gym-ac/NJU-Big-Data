import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyMain {
    public static void main(String[] args) throws Exception{
        //args: 命令行参数，分别为:
        //0:Job1输入数据集路径
        //1:Job1输出路径
        //2:Job2输出路径
        //3:Task 1输出的结果文件(放入distributed cache中)
        //4:Partition File保存路径

            //Job1：计算分数
                Configuration conf=new Configuration();
                
                Job job = Job.getInstance(conf,"Player Efficiency");

                job.setJarByClass(MyMain.class);
                job.setMapperClass(MyMapper.class);
                job.setReducerClass(MyReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));//输入数据集的路径
                job.addCacheFile(new Path(args[3]).toUri()); //distributed cache
                FileOutputFormat.setOutputPath(job,new Path(args[1]));//输出路径

                job.waitForCompletion(true); //执行MapReduce Job，等待结束


            //Job2:全局排序
                conf = new Configuration();
                //设置分区的排序策略，全局有序,而非每个分区内有序
                conf.set("mapreduce.totalorderpartitioner.naturalorder", "false");
                
                job = Job.getInstance(conf,"Global Sort");
                job.setJarByClass(MyMain.class);
                
                job.setMapperClass(GlobalSortMapper.class);
                job.setReducerClass(GlobalSortReducer.class);
                job.setPartitionerClass(TotalOrderPartitioner.class);
                job.setSortComparatorClass(KeyComparator.class);
    
                job.setInputFormatClass(KeyValueTextInputFormat.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class); 
     
                FileInputFormat.addInputPath(job, new Path(args[1]));//Job1输出路径
                FileOutputFormat.setOutputPath(job, new Path(args[2]));//Job2输出路径
                
                TotalOrderPartitioner.setPartitionFile(conf, new Path(args[4]));
                InputSampler.Sampler<Text,Text> sampler=new InputSampler.RandomSampler<>(0.01,1000,10);
                InputSampler.writePartitionFile(job, sampler);
    
                job.waitForCompletion(true); //执行MapReduce Job，等待结束        
    }
}
