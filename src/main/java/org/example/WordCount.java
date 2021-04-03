package org.example;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
   Basic MapReduce Programming.
 */
public class WordCount extends Configured implements Tool {

    /*  Define Writable data types in Hadoop MapReduce.
     *    : https://data-flair.training/forums/topic/define-writable-data-types-in-hadoop-mapreduce/
     *
     */

    /* Starting point */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WordCount(), args);
    }

    /* Mapper Class */
    public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {

        Text word = new Text();
        IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            // value.toString() 바꿔야 자바에서 자료 관리가 가능하므로 변경
            // StringTokenizer<String, 구분자>, 구분자 맨 뒤에 공백 있음.
            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                word.set(st.nextToken());
                context.write(word, one);
                // hasMoreTokens() : 반복문 돌면서 'hello hadoop world'
                // -> 3개로 끊어짐. 'hello', 'hadoo', 'world'
                // word.set : 하나씩 담음
                // nextToken : 다음 토큰 가져옴
                // context.write(word, one) : 단어 기록. (ex) 'hello'가 1번 나왔다 : ('hello', 1)
            }
        }
    }

    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable oval = new IntWritable();

        // Iterable : 반복자,IntWritable 클래스 타입은 반복을 가짐.
        // Iterable<IntWritable> --> key + value List(배열로) 들어온다고 생각(Shuffling ). (ex) hadoop[1, 1]
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            oval.set(sum);
            context.write(key, oval);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration(getConf());
        System.out.println(">>>>>>>>> Start Up WordCount <<<<<<<<<");

        // 실행시 외부에서 받은 first 인자 :  Configuration 실행을 위한 configuration 정보.(hadoop namenode, core-site.xml)
        // 실행시 외부에서 받은 second 인자 :  args[]
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: map-reduce wordcount <input dir> <output dir>");
            System.exit(-1);
        }

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        System.out.println("output path : " + output.toString());

        // config 및  Job 이름 설정
        Job myjob = Job.getInstance(getConf(), "my first job");
        myjob.setJarByClass(WordCount.class);
        myjob.setMapperClass(WCMapper.class);
        myjob.setReducerClass(WCReducer.class);
        myjob.setMapOutputKeyClass(Text.class);
        myjob.setMapOutputValueClass(IntWritable.class);
        myjob.setOutputFormatClass(TextOutputFormat.class);
        myjob.setInputFormatClass(TextInputFormat.class);

        // Job의 input Path
        // Job의 output Path
        FileInputFormat.addInputPath(myjob, input);
        FileOutputFormat.setOutputPath(myjob, output);

        myjob.waitForCompletion(true);

        return 0;
    }

}