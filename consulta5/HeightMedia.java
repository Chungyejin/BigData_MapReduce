package olympics.consulta5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class HeightMedia {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = Job.getInstance(conf, "mediaheight");
        job.setJarByClass(HeightMedia.class);
        job.setMapperClass(MediaMapper.class);
        job.setReducerClass(MediaReducer.class);
        job.setReducerClass(MediaCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HeightMediaWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MediaMapper extends Mapper<LongWritable, Text, Text, HeightMediaWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] values = line.split(",");

            if (values.length > 0) {
                String altura = values[12]; // Assumindo que a altura está na coluna 13
                if (!altura.matches(".*[a-zA-Z].*") && !altura.isEmpty()) {
                        float alturaValue = Float.parseFloat(altura);
                        if (alturaValue > 0) {
                            HeightMediaWritable heightWritable = new HeightMediaWritable();
                            heightWritable.add(alturaValue);
                            con.write(new Text("media"), heightWritable);
                        }
                }
            }
        }
    }


    public static class MediaCombiner extends Reducer<Text, HeightMediaWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<HeightMediaWritable> values, Context con) throws IOException, InterruptedException {
            HeightMediaWritable result = new HeightMediaWritable();

            for (HeightMediaWritable val : values) {
                result.add(val.totalHeight);
            }


            float average = result.getAverage();
            con.write(key, new FloatWritable(average));
        }
    }


    public static class MediaReducer extends Reducer<Text, HeightMediaWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<HeightMediaWritable> values, Context con) throws IOException, InterruptedException {
            HeightMediaWritable result = new HeightMediaWritable();

            for (HeightMediaWritable val : values) {
                result.add(val.totalHeight);
            }

            float average = result.getAverage();
            con.write(key, new FloatWritable(average));
        }
    }


}

