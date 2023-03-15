import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task2 {
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        conf1.set("mapred.textoutputformat.separator", ";");

        Path in = new Path(args[0]);
        Path temp = new Path(args[1]);
        Path out = new Path(args[2]);
        Job job1 = Job.getInstance(conf1, "Phase 1");
        job1.setJarByClass(Task2.class);
        // 设置输入和输出目录
        FileInputFormat.setInputPaths(job1, in);
        FileOutputFormat.setOutputPath(job1, temp);

        // 设置Map、Partitioner和Reduce处理类
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);

        // 设置Map输出类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        if(!job1.waitForCompletion(true))
            return;

        //task 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Phase 2");
        job2.setJarByClass(Task2.class);
        // 设置输入和输出目录
        FileInputFormat.setInputPaths(job2, temp);
        FileOutputFormat.setOutputPath(job2, out);

        // 设置Map、Partitioner和Reduce处理类
        job2.setMapperClass(Mapper2.class);
        job2.setPartitionerClass(MyPartitioner.class);
        job2.setSortComparatorClass(MyComparator.class);
        job2.setReducerClass(Reducer2.class);
        job2.setNumReduceTasks(5);
        // 设置Map输出类型
        job2.setMapOutputKeyClass(TypeScore.class);
        job2.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);


        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}
