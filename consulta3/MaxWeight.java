package olympics.consulta3;

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

public class MaxWeight {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        // Criação do job e seu nome
        Job job = Job.getInstance(conf, "maxweightcount");
        job.setJarByClass(MaxWeight.class);
        job.setMapperClass(MaxWeight.MaxMapper.class);
        job.setReducerClass(MaxWeight.MaxReducer.class);
        job.setReducerClass(MaxWeight.MaxCombiner.class);




        // Configurar tipos de dados de saída
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true); // Evita reexclusão ao rodar novamente
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class MaxMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] values = line.split(",");

            if(values.length > 0){
                String weight = values[13];
                if(!weight.matches(".*[a-zA-Z].*") && !weight.isEmpty()){
                    float max = Float.parseFloat(weight);
                    con.write(new Text("max weight: "), new FloatWritable(max));
                }
            }
        }
    }

    public static class MaxCombiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {

            float max = Float.MIN_VALUE;
            for (FloatWritable val : values) {
                float value = val.get();
                if (value > max && value > 0) {
                    max = value;
                }
            }

            if (max > Float.MIN_VALUE) {
                con.write(key, new FloatWritable(max));
            }
        }
    }

    public static class MaxReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {

            float max = Float.MIN_VALUE;
            for (FloatWritable val : values) {
                float value = val.get();
                if (value > max && value > 0) {
                    max = value;
                }
            }

            if (max > Float.MIN_VALUE) {
                con.write(key, new FloatWritable(max));
            }
        }
    }

}
