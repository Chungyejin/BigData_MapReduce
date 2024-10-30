package olympics.consulta9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

public class IdadeMedia extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new IdadeMedia(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path intermediate = new Path(args[1]);
        Path output = new Path(args[2]);
        Job job1 = Job.getInstance(conf, "Calcular Idade");
        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(intermediate, true);
        FileOutputFormat.setOutputPath(job1, intermediate);

        // Define job1 classes
        job1.setJarByClass(IdadeMedia.class);
        job1.setMapperClass(MapIdade.class);
        job1.setCombinerClass(CombineIdade.class);
        job1.setReducerClass(ReduceIdade.class);

        // Define output key and value data types for Mapper and Reducer
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf, "Calculate Average Age");
            FileInputFormat.addInputPath(job2, intermediate);
            FileSystem.get(conf).delete(output, true);
            FileOutputFormat.setOutputPath(job2, output);

            // Define job2 classes
            job2.setJarByClass(IdadeMedia.class);
            job2.setMapperClass(MapIdadeAvg.class);
            job2.setCombinerClass(CombineIdadeAvg.class);
            job2.setReducerClass(ReduceIdadeAvg.class);

            // Define job2 output key and value data types for Mapper and Reducer
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IdadeMediaWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(FloatWritable.class);
            return job2.waitForCompletion(true) ? 0 : 1;
        }

        return 1;
    }

    public static class MapIdade extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split(",");

            if (values.length > 1) {
                String nome = values[1].replaceAll("\"", ""); // Extrair o nome
                String dataNascimento = values[values.length - 1].replaceAll("\"", ""); // Extrair a data de nascimento

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                LocalDate nascimento = LocalDate.parse(dataNascimento, formatter);
                LocalDate hoje = LocalDate.now();
                int idade = Period.between(nascimento, hoje).getYears();

                con.write(new Text(nome), new LongWritable(idade)); // Emitir nome e idade
            }
        }
    }

    public static class CombineIdade extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {
            for (LongWritable val : values) {
                con.write(key, val);
            }
        }
    }

    public static class ReduceIdade extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {

            for (LongWritable val : values) {
                con.write(key, val);
            }
        }
    }

    public static class MapIdadeAvg extends Mapper<LongWritable, Text, Text, IdadeMediaWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] string_Data = line.split("\\s+");
            if (string_Data.length > 1) {
                float idade = Float.parseFloat(string_Data[string_Data.length-1]);
                con.write(new Text("media idade = "), new IdadeMediaWritable(idade, 1));
            }
        }
    }

    public static class CombineIdadeAvg extends Reducer<Text, IdadeMediaWritable, Text, IdadeMediaWritable> {
        public void reduce(Text key, Iterable<IdadeMediaWritable> values, Context con) throws IOException, InterruptedException {
            float sum = 0f;
            int count = 0;
            for (IdadeMediaWritable val : values) {
                sum += val.getSum();
                count += val.getCount();
            }
            con.write(key, new IdadeMediaWritable(sum, count));
        }
    }

    public static class ReduceIdadeAvg extends Reducer<Text, IdadeMediaWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<IdadeMediaWritable> values, Context con) throws IOException, InterruptedException {
            float sum = 0f;
            int count = 0;
            for (IdadeMediaWritable val : values) {
                sum += val.getSum();
                count += val.getCount();
            }
            float avg = sum / count;
            con.write(key, new FloatWritable(avg));
        }
    }
}

