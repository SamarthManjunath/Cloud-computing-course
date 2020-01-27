/*
Name:Samarth Manjunath
UTA ID:1001522809
Subject: Advanced Database systems 
*/
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Vector;
import com.google.common.collect.HashBasedTable;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import java.io.*;
import java.nio.file.Paths;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;



class Point implements WritableComparable<Point> { //inherited class which used writablecomparable attributes and functions
    public Double x;
    public Double y;
    public Point()
    {
        this.x=0.0; //initializing the Point class variables to 0
        this.y=0.0;
    }


    public Point(double point_variable1,double point_variable2) //assigining the point class variables to new variables passed in point function.
    {
        this.x=point_variable1;
        this.y=point_variable2;
    }
    @Override
    public void readFields(DataInput i) throws IOException { //function to read the points
        // TODO Auto-generated method stub
        x = i.readDouble();
        y = i.readDouble();
    }
    @Override
    public void write(DataOutput out) throws IOException { // function to write the values 
        // TODO Auto-generated method stub
        out.writeDouble(x);
        out.writeDouble(y);
    }
    @Override
    public int compareTo(Point p) { 
        // TODO Auto-generated method stub
        if(Double.compare(this.x, p.x)==0)//returns positive if this.x is greater than p.x other negative if equal 0                            
            return (int) (this.y- p.y);               
        else 
            return (int) (this.x-p.x);
    }
    
            
    public String toString() //converts the 2 values x and y to a commma seperated string
    {
        return Double.toString(x)+","+Double.toString(y);
    }       
}

class Avg implements Writable{
public double sample_11;
public double sample_22;
public long count;

public Avg(){this.sample_11 = 0.0;this.sample_22 = 0.0;this.count = 0;}

Avg(double m,double n,long count){
    sample_11=m;
    sample_22=n;
    this.count=count;
}
public void write(DataOutput out) throws IOException {

    out.writeDouble(sample_11);
    out.writeDouble(sample_22);
    out.writeLong(count);
}
public void readFields(DataInput in) throws IOException {

    sample_11 = in.readDouble();
    sample_22=  in.readDouble();
    count=in.readLong();
}
}

public class KMeans {
    static Vector<Point> centroids = new Vector<Point>(100);
    static Hashtable<Point,Avg> table;

    public static class AvgMapper extends Mapper<Object,Text,Point,Avg> { //mapper function which takes the key values pair and finally breaks them and sends them to reducer.
        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {                           
            table = new Hashtable<>();
            String reference_string;
            URI[] paths = context.getCacheFiles();
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
            while((reference_string=reader.readLine())!=null)
            {                           
                Point p = new Point(Double.parseDouble(reference_string.split(",")[0]),Double.parseDouble(reference_string.split(",")[1]));
                centroids.add(p);
            }       
            centroids.firstElement();                    
        }
        @Override
        protected void cleanup ( Context context ) throws IOException,InterruptedException {
            Set<Point> k1=table.keySet();
            for (Point key: k1)
            context. write (key,table.get(key));
            }
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException
        {                       
            centroids.firstElement();           
            Scanner obj = new Scanner(value.toString()).useDelimiter(",");
            Point point_1 = new Point(obj.nextDouble(),obj.nextDouble()); 
            Point point_2 = new Point();            
            double result = 0;
            double minimum_distance = 999999;
            for(Point p:centroids)
            {                                                   
                result = Math.sqrt(Math.pow(Math.abs(p.x-point_1.x), 2)+Math.pow(Math.abs(p.y-point_1.y), 2));//Eucledian distances calculation
                if(result<minimum_distance)
                {                                       
                    point_2 = p;
                    minimum_distance = result;                                                          
                }               
                
            }
             if (table.get(point_2) == null)
            table.put(point_2,new Avg(point_1.x,point_1.y,1) );
            else
            table.put(point_2,new Avg(table.get(point_2).sample_11+point_1.x,table.get(point_2).sample_22+point_1.y,table.get(point_2).count+1 ));
            
        }
    }
    
    
    public static class AvgReducer extends Reducer<Point,Avg,Point,Object> {
        public void reduce(Point key,Iterable<Avg> values,Context context) throws IOException, InterruptedException //function to reduce the split data to get final results.
        {   
            double count=0;
            Point s = new Point();
            s.x=0.0;
            s.y=0.0;
            for(Avg z:values)
            {
                count=count+z.count;
                s.x+=z.sample_11;
                s.y+=z.sample_22;
            }
            s.x=s.x/count;
            s.y=s.y/count;            
            context.write(s,null); //final result where the reducer combines values from all mappers and writes them to output file                             
        }
        
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {//main function where the execution starts
        // TODO Auto-generated method stub  
            Configuration conf  = new Configuration();
            Job job = Job.getInstance();
            job.setJobName("k_means");
            job.setJarByClass(KMeans.class);
            job.addCacheFile(new Path(args[1]).toUri());
            job.setOutputValueClass(Object.class);
            job.setOutputKeyClass(Point.class);
            job.setMapOutputKeyClass(Point.class);
            job.setMapOutputValueClass(Avg.class);
            job.setMapperClass(AvgMapper.class);
            job.setReducerClass(AvgReducer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[2]));
            job.waitForCompletion(true);

    }

}



