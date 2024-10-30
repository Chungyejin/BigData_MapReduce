package olympics.consulta8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

public class MaxName {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] inputs = new GenericOptionsParser(c, args).getRemainingArgs();

        org.apache.hadoop.fs.Path input = new org.apache.hadoop.fs.Path(inputs[0]);
        org.apache.hadoop.fs.Path output = new org.apache.hadoop.fs.Path(inputs[1]);

        Job job = new Job(c, "aggregation");

        job.setJarByClass(MaxName.class);
        job.setMapperClass(MaxName.MapperMaxName.class);
        job.setReducerClass(MaxName.MaxNameReducer.class);
        job.setCombinerClass(MaxName.MaxNameCombiner.class);

        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(MaxNameWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MaxNameWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(c).delete(output, true); // não precisar reexcluindo pra rodar novamente
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0:1);
    }

    public static class MapperMaxName extends Mapper<LongWritable, Text, Text, MaxNameWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] values = line.split(",");

            if(values.length > 0){
                String country = values[7].replace("\"", "");
                String name = values[1];
                String height = values[12];
                if(!height.matches(".*[a-zA-Z].*") && !height.isEmpty() && !name.isEmpty()){
                    float max = Float.parseFloat(height);
                    con.write(new Text(country), new MaxNameWritable(name, max));
                }
            }
        }
    }

    public static class MaxNameCombiner extends Reducer<Text, MaxNameWritable, Text, MaxNameWritable> {
        public void reduce(Text key, Iterable<MaxNameWritable> values, Context con)
                throws IOException, InterruptedException{

            float max = -1000f;
            String name = "";

            for(MaxNameWritable val : values){

                float height = val.getMax_height();
                if(height > max){
                    max = height;
                    name = val.getAthlete();
                }
            }
            con.write(key, new MaxNameWritable(name, max));
        }
    }

    public static class MaxNameReducer extends Reducer<Text, MaxNameWritable, Text, MaxNameWritable> {
        public void reduce(Text key, Iterable<MaxNameWritable> values, Context con)
                throws IOException, InterruptedException{

            float max = -1000f;
            String name = "";

            for(MaxNameWritable val : values){
                float height = val.getMax_height();
                if(height > max){
                    max = height;
                    name = val.getAthlete();
                }
            }
            con.write(key, new MaxNameWritable(name, max));

        }
    }
}