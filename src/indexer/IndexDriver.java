package indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", "\t");
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(IndexDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(IndexMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(IndexReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		

//		KeyValueTextInputFormat.addInputPath(job, new Path("./input"));
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(0);
		System.out.println("finish set up");
		
		try{
			if (!job.waitForCompletion(true))
				System.out.println("not running");
			else{
				System.out.println("running");
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
