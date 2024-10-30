package olympics.consulta4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.time.LocalDate;


import java.io.IOException;

public class Birthday {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);


        Job job = Job.getInstance(conf, "youngestbirthday");
        job.setJarByClass(Birthday.class);
        job.setMapperClass(Birthday.BirthdayMapper.class);
        job.setReducerClass(Birthday.BirthdayReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class BirthdayMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] values = line.split(",");

            if (values.length > 1) {
                String birthday = values[values.length -1].replaceAll("\"", "");

                con.write(new Text("youngest"), new Text(birthday));
            }
        }
    }


    public static class BirthdayReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context con)
                throws IOException, InterruptedException {

            LocalDate youngestBirthday = null;

            for (Text val : values) {
                String birthdayS = val.toString();
                LocalDate birthday = LocalDate.parse(birthdayS);

                if (youngestBirthday == null || birthday.isAfter(youngestBirthday)) {
                    youngestBirthday = birthday;
                }

            }
            if (youngestBirthday != null) {
                con.write(null, new Text(youngestBirthday.toString()));
            }


        }

    }
}
