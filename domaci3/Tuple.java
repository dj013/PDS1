package pds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Tuple {

	public static class MinMaxCountTuple implements Writable {
		private int minSum = 0;
		private int maxSum = 0;
		private int countMin = 0;
		private int countMax = 0;
		
		public MinMaxCountTuple() {}
		
	
		
		public int getMin() { return minSum; }
		public int getMax() { return maxSum; }
		public int getCountMin() { return countMin; }
		public int getCountMax() { return countMax; }
		public void setMin(int min) { this.minSum = min; }
		public void setMax(int max) { this.maxSum = max; }
		public void setCountMin(int count) { this.countMin = count; }
		public void setCountMax(int count) { this.countMax = count; }
		@Override
		public void readFields(DataInput in) throws IOException {
			minSum = in.readInt();
			countMin = in.readInt();
			maxSum = in.readInt();
			countMax = in.readInt();			
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(minSum);
			out.writeInt(countMin);
			out.writeInt(maxSum);
			out.writeInt(countMax);
		}
		
		public String toString() {
			return "min: " + minSum*1.0/countMin + ", max: " + maxSum*1.0/countMax;
		}
		
	}
	
	public static class TupleMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {
		private Text month = new Text();
		private MinMaxCountTuple outTuple = new MinMaxCountTuple();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			month.set(line[1].substring(4,6));
			int temperature = Integer.parseInt(line[3]);
			String type = line[2];
			
			if(type.contentEquals("TMIN"))
			{
				outTuple.setMin(temperature);
				outTuple.setCountMin(1);
				
			}
			else if(type.contentEquals("TMAX"))
			{
				outTuple.setMax(temperature);
				outTuple.setCountMax(1);
			}
			
			//outTuple = new MinMaxCountTuple(temperature);
			
			context.write(month, outTuple);
		}
	}
	
	public static class TupleReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
		private MinMaxCountTuple result = new MinMaxCountTuple();		
		
		public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
			result.setCountMax(0);
			result.setCountMin(0);
			result.setMax(0);
			result.setMin(0);
			
			for (MinMaxCountTuple value : values) {
	
				result.setMin(result.getMin()+value.getMin());
				result.setMax(result.getMax()+value.getMax());
		
				result.setCountMax(result.getCountMax()+value.getCountMax());
				result.setCountMin(result.getCountMin()+value.getCountMin());
			}
			
			//result.setMin(result.getMin()*1.0/result.getCountMin());
			//result.setMax(result.getMax()*1.0/result.getCountMax());
			
			//result.setCountMax(1);
			//result.setCountMin(1);
			
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "min max count temperature");
		job.setJarByClass(Tuple.class);
		job.setMapperClass(TupleMapper.class);
		job.setCombinerClass(TupleReducer.class);
		job.setReducerClass(TupleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxCountTuple.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
 
