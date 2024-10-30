package olympics.consulta6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class FPercentageByCountryDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int reducersQuantity = Integer.parseInt(args[2]);

        Job job = Job.getInstance(conf);
        job.setJobName("GenderPercentageByCountry");
        job.setNumReduceTasks(reducersQuantity);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
        job.setJarByClass(FPercentageByCountryDriver.class);
        job.setMapperClass(GenderMapper.class);
        job.setCombinerClass(GenderCombiner.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new FPercentageByCountryDriver(), args);
        System.exit(result);
    }

    public static class GenderMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",");

            if (columns.length > 9 && !columns[9].isEmpty() && !columns[9].equals("0") && columns.length > 4 && !columns[4].isEmpty()) {
                String country = columns[9].trim().replace("\"", "");;
                String gender = columns[4].trim();

                context.write(new Text(country), new Text(gender));
            }
        }
    }

    public static class GenderCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maleCount = 0;
            int femaleCount = 0;

            for (Text value : values) {
                if (value.toString().equalsIgnoreCase("Male")) {
                    maleCount++;
                } else if (value.toString().equalsIgnoreCase("Female")) {
                    femaleCount++;
                }
            }

            context.write(key, new Text(maleCount + "," + femaleCount));
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maleCount = 0;
            int femaleCount = 0;

            for (Text value : values) {
                String[] counts = value.toString().split(",");
                maleCount += Integer.parseInt(counts[0]);
                femaleCount += Integer.parseInt(counts[1]);
            }

            int totalCount = maleCount + femaleCount;
            if (totalCount > 0) {
                double malePercentage = (maleCount * 100.0) / totalCount;
                double femalePercentage = (femaleCount * 100.0) / totalCount;
                context.write(new Text(key.toString()), new Text(String.format("Female: %.2f%%", femalePercentage)));
            } else {
                context.write(key, new Text("No athletes"));
            }
        }
    }
}

