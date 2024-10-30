package olympics.consulta1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class CountCountries {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);
        // arquivo de saida
        Path output = new Path(files[1]);


        // criacao do job e seu nome
        Job job = new Job(conf, "countries");
        job.setJarByClass(CountCountries.class);
        job.setMapperClass(CountCountries.MapCountryCount.class);
        job.setReducerClass(CountCountries.ReduceCountryCount.class);

        //configurar tipos de dados de saída
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true); // não precisar reexcluindo pra rodar novamente
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapCountryCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] string_Data = line.split(",");

            if(string_Data.length > 0) {
                String country = string_Data[7].replace("\"", "");
                if(!country.isEmpty()){
                    con.write(new Text(country), new IntWritable(1));
                }
            }
        }
    }

    public static class ReduceCountryCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable valor: values){
                count += valor.get();
            }
            con.write(key, new IntWritable(count));
        }
    }
}