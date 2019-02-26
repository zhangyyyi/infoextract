package infoextract;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;  
import java.lang.*;  
   
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
//import org.apache.hadoop.util.GenericOptionsParser;  
   
public class loganalysis {  
   
  public static class TokenizerMapper   
       extends Mapper<Object, Text, Text, IntWritable>{  
   
//  private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String imei = new String();  //username
    private String areacode  = new String();  //returnurl
//  private String responsedata = new String();  
    private String requesttime = new String();  //loginStartTime
//  private String requestip = new String();  
  
//  map阶段的key-value对的格式是由输入的格式所决定的，如果是默认的TextInputFormat，则每行作为一个记录进程处理，其中key为此行的开头相对于文件的起始位置，value就是此行的字符文本  
//  map阶段的输出的key-value对的格式必须同reduce阶段的输入key-value对的格式相对应  
public void map(Object key, Text value, Context context  
                ) throws IOException, InterruptedException {  
  //StringTokenizer itr = new StringTokenizer(value.toString()); 
    	
      
	  String line = null;
//	  Text text = transformTextToUTF8(value, "GBK"); // 将GBK格式统一转换为 UTF-8
//	  line = text.toString();
	  
	  line = value.toString();
    	
      int areai = line.indexOf("\"username\"", 94);  
      int imeii = line.indexOf("\"returnurl\"", 94);  
      int retimei = line.indexOf("\"loginStartTime\"", 94);  
        
      if (areai==-1)  
      { 
    	  areacode=""; 
      }  
      else  
      {  
		  areacode=line.substring(areai+13);  
		  int len2=areacode.indexOf("]");  
		  if(len2 <= 1)  
			{  
			  areacode="";  
			}  
		  else   
		    {  
		      areacode=areacode.substring(0,len2);  
		    }  
        
      }  
      
      if (imeii==-1)  
      { 
    	  imei=""; 
      }  
      else  
      {  
          imei=line.substring(imeii+12);  
	      int len2=imei.indexOf("]");  
	      if(len2 < 1)  
	        {  
	          imei="";  
	        }  
	      else   
	        {  
	          imei=imei.substring(0,len2);  
	        }  
	        
      }
      
      if (retimei==-1)  
      { 
    	  requesttime=""; 
      }  
      else  
      {  
          requesttime=value.toString().substring(retimei+17);  
	      int len2=requesttime.indexOf("]");  
	      if(len2 <= 1)  
	        {  
	          requesttime="";  
	        }  
	      else   
	        {  
	          requesttime=requesttime.substring(0,len2);  
	        }  
	        
      }        

     /* while (itr.hasMoreTokens()) { 
          string tim; 
           
        word.set(itr.nextToken()); 
        context.write(word, one); 
      }*/  
     if(imei!=""&&areacode!=""&&requesttime!="")//&&responsedata!=""&&requestip!="")  
     {  
	   String wd=new String();	   
	   IntWritable asum = new IntWritable(Integer.valueOf(imei).intValue());
	   wd=areacode;//+"\t"+imei+responsedata+"\t"+requesttime+"\t"+requestip;  
	   //wd="areacode|"+areacode +"|imei|"+ imei +"|responsedata|"+ responsedata +"|requesttime|"+ requesttime +"|requestip|"+ requestip;  
	   word.set(wd);  
	   context.write(word, asum);  
     }  
  
    }
   
    public Text transformTextToUTF8(Text text, String encoding) {
  	  String value = null;
  	  try {
  	  value = new String(text.getBytes(), 0, text.getLength(), encoding);
  	  } catch (UnsupportedEncodingException e) {
  	  e.printStackTrace();
  	  }
  	  return new Text(value);
  	  }
  	
  }  
   
  public static class IntSumReducer   
       extends Reducer<Text,IntWritable,Text,IntWritable> {  
    private IntWritable result = new IntWritable();  
   
    public void reduce(Text key, Iterable<IntWritable> values,   
                       Context context  
                       ) throws IOException, InterruptedException {  
      int sum = 0;  
      for (IntWritable val : values) {  
        sum += val.get();  
      }  
      result.set(sum);  
      context.write(key, result);  
    }  
  }
  
  
  public static void main(String[] args) throws Exception {  
    Configuration conf = new Configuration();
      
    if (args.length != 2) {  
      System.err.println("Usage: wordcount <in> <out>");  
      System.exit(2);  
    }
    System.out.println(args[0]);
    System.out.println(args[1]);
      
    //Job job = new Job(conf, "word count");  
    Job job = Job.getInstance(conf);      
    job.setJarByClass(loganalysis.class);
    //设置map和reduce处理类
    job.setMapperClass(TokenizerMapper.class);  
    job.setCombinerClass(IntSumReducer.class);  
    job.setReducerClass(IntSumReducer.class);
    //设置输出键值类
    job.setOutputKeyClass(Text.class);  
    job.setOutputValueClass(IntWritable.class);
    //设置为文件读取输出类
    FileInputFormat.addInputPath(job, new Path(args[0]));  
    FileOutputFormat.setOutputPath(job, new Path(args[1]));  
    System.exit(job.waitForCompletion(true) ? 0 : 1);  
  }  
}  