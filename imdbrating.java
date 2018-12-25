import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class imdbrating {
    // Class to implement the mapper interface
    static class imdbmapper extends Mapper<LongWritable, Text, Text, Text> {
        // Map interface of the MapReduce job
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Get the current line
            String line = value.toString();
            // Line is of the format <[Total Votes] [Rating] [Movie] [Year]>
		//data_used, semester, first_name, usn 
		//ug_per,course,id,fname
            String[] line_values = line.split(",");

            // Keep only movies which have registered 30,000 or more votes
		// Keep only those having data_used > 40.
            if (Integer.parseInt(line_values[0]) > 90)
                context.write(new Text(line_values[3]), new Text(line_values[0] + "\t" + line_values[1] + "\t" + line_values[2]));
        }
    }
    // Class to implement the reducer interface
    static class imdbreducer extends Reducer<Text, Text, Text, Text> {
        // Reduce interface of the MapReduce job
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Vars to hold the highest values
            float max_rating = Float.MIN_VALUE;
            int max_votes = Integer.MIN_VALUE;
            String[] line_values;
            String output = "";
            int vote;
            float rating;

            // Iterate through each value
            for (Text value: values) {
                // value is of the format <[Total Votes] [Rating] [Movie]>
                line_values = value.toString().split("\t");

                // Get the numerical values of a votes & a ratings
                rating = Float.parseFloat(line_values[1]);
                vote = Integer.parseInt(line_values[0]);

                // Check if the current rating is more than max_rating
                if (rating > max_rating) {
                    max_rating = rating;
                    max_votes = vote;

                    output = line_values[2] + "\t" + line_values[1];
                }
                else if (rating == max_rating) {
                    // In case of tie, decide by the number of votes
                    if (vote > max_votes) {
                        max_rating = rating;
                        max_votes = vote;

                        output = line_values[2] + "\t" + line_values[1];
                    }
                }
            }
            // Write the output
            context.write(key, new Text(output));
        }
    }
    // Main method
    public static void main(String[] args) throws Exception {
        // Check if the arguments are right
        if (args.length != 2) {
            System.err.println("Usage - imdbrating <input-file> <output-path>");
            System.exit(-1);
        }

        // Create a job for the mapreduce task
        Job job = new Job();
        job.setJarByClass(imdbrating.class);

        // Set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the mapper and reducer class
        job.setMapperClass(imdbmapper.class);
        job.setReducerClass(imdbreducer.class);

        // Set the key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Wait for the job to finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
