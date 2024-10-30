package olympics.consulta10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class IMCDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new IMCDriver(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path intermediate = new Path(args[1]);
        Path output = new Path(args[2]);
        Job job1 = Job.getInstance(conf);
        job1.setJobName("Count-Characters");
        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(intermediate, true);
        FileOutputFormat.setOutputPath(job1, intermediate);
        //define job1 classes
        job1.setJarByClass(IMCDriver.class);
        job1.setMapperClass(IMCDriver.MapIMC.class);
        job1.setCombinerClass(IMCDriver.CombineIMC.class);
        job1.setReducerClass(IMCDriver.ReduceIMC.class);
        //define output key and value data types for Mapper and Reducer
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IMCWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);

        if(job1.waitForCompletion(true)){
         Job job2 = Job.getInstance(conf);
         job2.setJobName("IMC average");
         FileInputFormat.addInputPath(job2,intermediate);
         FileSystem.get(conf).delete(output, true);
         FileOutputFormat.setOutputPath(job2, output);
         //define job2 classes
         job2.setJarByClass(IMCDriver.class);
         job2.setMapperClass(IMCDriver.MapAvg.class);
         job2.setCombinerClass(IMCDriver.CombineAvg.class);
         job2.setReducerClass(IMCDriver.ReduceAvg.class);
         // define job2 output key and value data types for Mapper and Reducer
         job2.setMapOutputKeyClass(Text.class);
         job2.setMapOutputValueClass(IMCAvgWritable.class);
         job2.setOutputKeyClass(Text.class);
         job2.setOutputValueClass(FloatWritable.class);
         return job2.waitForCompletion(true) ? 0 : 1;
        }

         return 1;
    }

    public static class MapIMC extends Mapper<LongWritable, Text, Text, IMCWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] string_Data = line.split(",");
            if(string_Data.length > 0){
                String nome = string_Data[2];
                String saltura = string_Data[12];
                String speso = string_Data[13];
                if(!saltura.matches(".*[a-zA-Z].*") && !saltura.isEmpty() && !speso.matches(".*[a-zA-Z].*") && !speso.isEmpty()){
                    float altura = Float.parseFloat(saltura);
                    float peso = Float.parseFloat(speso);
                    if(altura > 0 && peso > 0) con.write(new Text(nome + " ; "), new IMCWritable(altura, peso));
                }
            }
        }
    }
    public static class CombineIMC extends Reducer<Text, IMCWritable, Text, IMCWritable>{
        public void reduce(Text key, Iterable<IMCWritable> values, Context con)
                throws IOException, InterruptedException {

            for(IMCWritable val : values){
                float altura = val.getAltura() / 100.0f;
                float peso = val.getPeso();

                con.write(key, new IMCWritable(altura, peso));
            }
        }
    }
    public static class ReduceIMC extends Reducer<Text, IMCWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<IMCWritable> values, Context con)
                throws IOException, InterruptedException {
            for(IMCWritable val : values){
                float altura = val.getAltura();
                float peso = val.getPeso();
                float IMC = peso / (altura * altura);
                System.out.println(key + " " + altura + " " + peso + " " + IMC);

                con.write(key, new FloatWritable(IMC));
            }
        }
    }


    public static class MapAvg extends Mapper<LongWritable, Text, Text, IMCAvgWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] string_Data = line.split(";");
            if(string_Data.length > 0){
                    float imc = Float.parseFloat(string_Data[1]);
                    con.write(new Text("media imc = "), new IMCAvgWritable(imc, 1));
            }
        }
    }

    public static class CombineAvg extends Reducer<Text, IMCAvgWritable, Text, IMCAvgWritable> {
        public void reduce(Text key, Iterable<IMCAvgWritable> values, Context con)
                throws IOException, InterruptedException {
            float sumimc = 0f;
            int count = 0;
            for(IMCAvgWritable val : values){
                sumimc += val.getSumIMC();
                count += val.getCount();
            }
            con.write(key, new IMCAvgWritable(sumimc, count));
        }
    }

    public static class ReduceAvg extends Reducer<Text, IMCAvgWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<IMCAvgWritable> values, Context con)
                throws IOException, InterruptedException {
            float sumimc = 0f;
            int count = 0;
            for(IMCAvgWritable val : values){
                sumimc += val.getSumIMC();
                count += val.getCount();
            }
            float avg = sumimc/ count;
            con.write(key, new FloatWritable(avg));
        }
    }
}
