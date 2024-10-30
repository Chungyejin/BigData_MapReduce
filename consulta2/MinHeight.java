package olympics.consulta2;

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

public class MinHeight {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);
        // arquivo de saida
        Path output = new Path(files[1]);


        // criacao do job e seu nome
        Job job = new Job(conf, "min height");
        job.setJarByClass(MinHeight.class);
        job.setMapperClass(MinHeight.MinMapper.class);
        job.setReducerClass(MinHeight.MinReducer.class);
        job.setCombinerClass(MinHeight.MinCombiner.class);

        //configurar tipos de dados de saída
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true); // não precisar reexcluindo pra rodar novamente
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);//O código retorna 0 se o job foi bem-sucedido, ou 1 em caso de falha.
    }

    public static class MinMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] values = line.split(",");

            if(values.length > 0){
                String height = values[12];
                if(!height.matches(".*[a-zA-Z].*") && !height.isEmpty()){
                    float min = Float.parseFloat(height);
                    con.write(new Text("min height: "), new FloatWritable(min));
                }
            }
        }
    }

    public static class MinCombiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException{

            float min = 500f;
            for(FloatWritable val : values){
                float value = val.get();
                if(value < min && value > 0) min = value;
            }
            con.write(key, new FloatWritable(min));
        }
    }

    public static class MinReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException{

            float min = 500f;
            for(FloatWritable val : values){
                float value = val.get();
                if(value < min && value > 0) min = value;
            }
            con.write(key, new FloatWritable(min));
        }
    }
}
