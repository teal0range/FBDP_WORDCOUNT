import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Pattern;


public class MapReduce extends Configured implements Tool{

    public static class TokenizerMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        public static List<String> punctuation = new ArrayList<>();
        public static List<String> stopWords = new ArrayList<>();
        private static boolean isInit = false;


        public static void initStopWords() throws IOException{
            if(!isInit){
                // load punctuation
                InputStream file = MapReduce.class.getClassLoader().getResource("punctuation.txt").openStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(file));
                String line = null;
                while ((line = br.readLine())!=null){
                    punctuation.add(line.replaceFirst("\\\\", ""));
                }
                // load stop words
                br.close();
                file = MapReduce.class.getClassLoader().getResource("stop-word-list.txt").openStream();
                br = new BufferedReader(new InputStreamReader(file));
                while((line = br.readLine())!=null){
                    stopWords.add(line.trim());
                }
                isInit=true;
            }
        }

        public static String removePunctuation(String token){
            for (String word:punctuation) {
                token = token.replace(word, "");
            }
            return token;
        }

        public static boolean validToken(String token){
            Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
            if(pattern.matcher(token).matches()){
                return false;
            }
            for (String word:stopWords) {
                if(token.equals(word)){
                    return false;
                }
            }
            return true;
        }

        @Override
        public void map(LongWritable longWritable, Text text,
                        OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            initStopWords();
            String value = removePunctuation(text.toString().toLowerCase());
            StringTokenizer str = new StringTokenizer(value);
            while (str.hasMoreTokens()){
                String token = str.nextToken();
                if( validToken(token)){
                    text.set(removePunctuation(token));
                    outputCollector.collect(text, one);
                }
            }
        }
    }

    public static class TokenizerReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                Reporter reporter) throws IOException {
            int sum = 0;
            while(values.hasNext()){
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }

    }

    public static class SortMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
        private final IntWritable sum = new IntWritable();
        private final Text text = new Text();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
                throws IOException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            text.set(itr.nextToken().toString());
            sum.set(Integer.parseInt(itr.nextToken().toString()));
            output.collect(sum, text);                    
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
         }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
          }
    }

    public static void main(String[] args) throws Exception {
        TokenizerMapper.initStopWords();
        int res = ToolRunner.run(new Configuration(), new MapReduce(), args);
        System.exit(res);
        
        // System.out.println(TokenizerMapper.isInit);
        // System.out.println(TokenizerMapper.stopWords.size());
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), MapReduce.class);
        conf.setJobName("wordcount");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        conf.setMapperClass(TokenizerMapper.class);
        conf.setCombinerClass(TokenizerReducer.class);
        conf.setReducerClass(TokenizerReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path("word-count-temp"));

        RunningJob job = JobClient.runJob(conf);
        job.waitForCompletion();

        conf = new JobConf(getConf(), MapReduce.class);
        conf.setJobName("sort");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setMapperClass(SortMapper.class);
        conf.setOutputKeyComparatorClass(IntWritableDecreasingComparator.class);
        // conf.setCombinerClass(SortReducer.class);
        // conf.setReducerClass(SortReducer.class);
        // conf.setCombinerKeyGroupingComparator(IntWritableDecreasingComparator.class);
        conf.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(conf, new Path("word-count-temp"));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        JobClient.runJob(conf);

        return 0;
    }


}
