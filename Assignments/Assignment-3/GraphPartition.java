/*
Name: Samarth Manjunath
UTA ID: 1001522809
Subject: Advanced database systems.
Assignment-3
*/
import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent = new Vector<Long>();     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    /* ... */
    public Vertex()
    {
    	adjacent = new Vector<>();
    	this.id=0;
    	this.adjacent=null;
    	this.centroid=-1;
    	this.depth=0;
    }
    public Vertex(long id, Vector<Long> jda, long centroid, short depth)
    {
    	adjacent = new Vector<>();
    	this.id = id;
    	this.adjacent = jda;
    	this.centroid = centroid;
    	this.depth = depth;    	
    }
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		adjacent = new Vector<>();
		this.id = in.readLong();
		this.centroid = in.readLong();		
		this.depth = in.readShort();
		long sample1 = in.readLong();
		for(int i=0;i<sample1;i++)
			this.adjacent.addElement(in.readLong());
				
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(id);
		out.writeLong(centroid);
		out.writeShort(depth);
		out.writeLong(adjacent.size());
		for(int i=0;i<adjacent.size();i++)
		{
			out.writeLong(adjacent.get(i));			
		}				
	}
	public String toString()
	{
		if(adjacent==null)
			return Long.toString(id)+","+ null+","+Long.toString(centroid)+","+Short.toString(depth);
		return Long.toString(id)+","+ adjacent.toString()+","+Long.toString(centroid)+","+Short.toString(depth);
	}
       
}

public class GraphPartition {
    static Vector<Long> final_cent = new Vector<Long>();
    final static short maximum_deep = 8;
    static short breadth_first_search_depth = 0;
    static int number=0;
    /* ... */
    public static class GraphMapper1 extends Mapper<Object,Text,LongWritable,Vertex>
    {
    	private LongWritable id = new LongWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    	{
    		Scanner sscan = new Scanner(value.toString()).useDelimiter(",");    		
    		long centroid = -1;    		
    		long identifier1 = sscan.nextLong(); 
    		id.set(identifier1);    			
    		Vector<Long> jda = new Vector<Long>();
    		while(sscan.hasNext())
    			jda.add(sscan.nextLong());
    		if(number<10)
    		{    			
    			centroid = identifier1;
    			final_cent.add(identifier1);
    			number++;    			
    			context.write(id, new Vertex(identifier1,jda,centroid,breadth_first_search_depth));
    		}  
    		else
    			context.write(id, new Vertex(identifier1,jda,centroid,breadth_first_search_depth));
    	}
    }   

    public static class GraphReducer1 extends Reducer<LongWritable,Vertex,LongWritable,Vertex>
    {
    	public void reduce(LongWritable key, Iterable<Vertex> values,Context context) throws IOException, InterruptedException
    	{
    		short minimum_deep=1000;    		    		
    		Vertex sam = new Vertex(key.get(),new Vector<>(),-1,(short)0);
    		for(Vertex v:values)
    		{    			
    			if(!v.adjacent.isEmpty())
    				sam.adjacent=v.adjacent;    			
    			if(v.centroid>0 && v.depth<minimum_deep)
    			{
    				minimum_deep = v.depth;
    				sam.centroid = v.centroid;
    			}
    		}
    		sam.depth = minimum_deep;
    		context.write(key,sam);
    	}
    } 
    
    public static class GraphMapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex>
    {    	
		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException
    	{    						
			Vector<Long> jdb = new Vector<Long>();						
    		context.write(new LongWritable(value.id), value);    		
    		if(value.centroid>0)    		
    			for(long l:value.adjacent)    			    		
    				context.write(new LongWritable(l), new Vertex(l,jdb,value.centroid,breadth_first_search_depth));    		
    	}
    }
    
    public static class GraphMapper3 extends Mapper<LongWritable, Vertex, LongWritable, IntWritable>
    {
    	public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException
    	{    		    		    		   		    		 		    		    		
    		if(value.centroid>=0)
    			context.write(new LongWritable(value.centroid), new IntWritable(1));
    	}
    }
    public static class GraphReducer3 extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable>
    {
    	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    	{
    		int sam=0;
    		for(IntWritable v:values)
    			sam=sam+v.get();
    		context.write(key, new IntWritable(sam));
    	}
    }
    
    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(GraphPartition.class);
        /* ... First Map-Reduce job to read the graph */
        job.setMapperClass(GraphMapper1.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < maximum_deep; i++ ) {
            breadth_first_search_depth++;
            job = Job.getInstance();
	job.setJarByClass(GraphPartition.class);
            job.setMapperClass(GraphMapper2.class);
            job.setReducerClass(GraphReducer1.class);            
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job, new Path(args[1]+"/i"+i));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/i"+(i+1)));
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the cluster sizes */      
job.setJarByClass(GraphPartition.class);
        job.setMapperClass(GraphMapper3.class);
        job.setReducerClass(GraphReducer3.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}
