package hello;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.huaban.analysis.jieba.*;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;

public class Pinyin {
    public static class PinyinMapper1 extends Mapper<Object, Text, Text, Text> {
    	private static final char[] ban = {'。','，','：','“','”','‘','’','？','！','／','、','【','】','「','」','%','¥','…',' '};
    	private static final Text one = new Text("1");
    	private Text word = new Text();
//    	private boolean isChinese(char charecter) {
//    		for (int i = 0; i < ban.length; i++) {
//       			if (charecter == ban[i]) return false;
//        	}
//    		if (charecter < 0x4e00 || charecter > 0x9fbb) return false;
//    		return true;
//    	}
    	private boolean isChinese(String str) {
    		for (int i = 0; i < str.length(); i++) {
    			char charecter = str.charAt(i);
    			for (int j = 0; j < ban.length; j++) {
    				if (charecter == ban[i]) return false;
    			}
    			if (charecter < 0x4e00 || charecter > 0x9fbb) return false;
    		}
    		return true;
    	}
    	
        public void map(Object key, Text value, Context context) 
        		throws IOException, InterruptedException {
        	String line = new String(value.getBytes(), 0, value.getLength(), "GB18030");
        	if (line.length() > 9) {
        		if (line.substring(0, 9).equals("<content>")) {
                	String tmpContent = new String(line.substring(9, line.length() - 10));
                	if (tmpContent.length() > 0) {
                		JiebaSegmenter segmenter = new JiebaSegmenter();
                    	List<String> content = segmenter.sentenceProcess(tmpContent);
                		boolean flag = isChinese(content.get(0));
                    	for (int i = 1; i < content.size(); i++) {
                    		if (isChinese(content.get(i))) {
                    			if (flag) {
                    				String tmpKey = content.get(i - 1) + String.valueOf(':') + content.get(i);
                    				word.set(tmpKey);
                    				if (content.get(i - 1).length() < 5) {
                    					context.write(word, one);
                    				}
                    			}
                    			flag = true;
                    		} else {
                    			flag = false;
                    		}
                    	}
                	}
                }
        	}
        }
    }

    public static class PinyinReducer1 extends Reducer<Text, Text, Text, Text> {
    	private Text result1 = new Text(), result2 = new Text();
    	public void reduce(Text key, Iterable<Text> values, Context context)
    			throws IOException, InterruptedException {
    		int sum = 0;
    		for (Text i : values) {
    			sum++;
    		}
    		int split = key.toString().indexOf(':');
    		result1.set(key.toString().substring(0, split));
    		result2.set(key.toString().substring(split + 1) + String.valueOf(':') + String.valueOf(sum));
    		context.write(result1, result2);
    	}
    	
    }
    
    public static class PinyinMapper2 extends Mapper<Object, Text, Text, Text> {
    	private Text tmpKey = new Text(), tmpVal = new Text();
    	public void map(Object key, Text value, Context context) 
        		throws IOException, InterruptedException {
    		int splitKey = value.toString().indexOf('\t');
    		if (splitKey != -1) {
    			tmpKey.set(value.toString().substring(0, splitKey));
    			tmpVal.set(value.toString().substring(splitKey + 1));
    			context.write(tmpKey, tmpVal);
    		}
    	}
    }
    
    public static class PinyinReducer2 extends Reducer<Text, Text, Text, Text> {
    	private Text tmpVal = new Text();
    	public void reduce(Text key, Iterable<Text> values, Context context)
    			throws IOException, InterruptedException {
    		List<String> recordsList = new ArrayList<String>();
    		String records = new String();
    		for (Text i : values) {
    			recordsList.add(i.toString());
    		}
    		Collections.sort(recordsList, new Comparator<String>() {
    			@Override  
    			public int compare(String ele1, String ele2) {
    				long num1 = Long.parseLong(ele1.substring(ele1.indexOf(':') + 1));
    				long num2 = Long.parseLong(ele2.substring(ele2.indexOf(':') + 1));
    				if (num1 > num2) return -1;
    				else if (num1 < num2) return 1;
    				else return 0;
    			}
    		});
    		int times = 0;
    		for (String i : recordsList) {
    			records += i + String.valueOf("; ");
    			if (times++ > 4) break;
    		}
    		tmpVal.set(records);
    		context.write(key, tmpVal);
    	}
    }
    
    public static void main(String[] args) throws IOException {	
		JobConf conf = new JobConf(Pinyin.class);
		//第一个job的配置
		Job job1 = new Job(conf,"join1");
		job1.setJarByClass(Pinyin.class); 
	    job1.setMapperClass(PinyinMapper1.class); 
	    job1.setReducerClass(PinyinReducer1.class); 
		job1.setMapOutputKeyClass(Text.class); 
		job1.setMapOutputValueClass(Text.class); 
		job1.setOutputKeyClass(Text.class); 
		job1.setOutputValueClass(Text.class); 
		ControlledJob ctrljob1=new  ControlledJob(conf); 
		ctrljob1.setJob(job1); 
		FileInputFormat.addInputPath(job1, new Path(args[0])); 
	    FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_tmp")); 

	    //第二个作业的配置
    	Job job2=new Job(conf,"Join2"); 
	    job2.setJarByClass(Pinyin.class); 	    
	    job2.setMapperClass(PinyinMapper2.class); 
	    job2.setReducerClass(PinyinReducer2.class); 
		job2.setMapOutputKeyClass(Text.class); 
		job2.setMapOutputValueClass(Text.class); 
		job2.setOutputKeyClass(Text.class); 
		job2.setOutputValueClass(Text.class);  
		ControlledJob ctrljob2=new ControlledJob(conf); 
		ctrljob2.setJob(job2); 
	
		ctrljob2.addDependingJob(ctrljob1); 		
		FileInputFormat.addInputPath(job2, new Path(args[1] + "_tmp"));		
		FileOutputFormat.setOutputPath(job2,new Path(args[1]));
		JobControl jobCtrl=new JobControl("myctrl"); 
		jobCtrl.addJob(ctrljob1); 
		jobCtrl.addJob(ctrljob2); 
		Thread  t=new Thread(jobCtrl); 
		t.start(); 
		while (true) { 
			if(jobCtrl.allFinished()) { 
				System.out.println(jobCtrl.getSuccessfulJobList()); 
				jobCtrl.stop(); 
				break; 
			}
		}
	}
}