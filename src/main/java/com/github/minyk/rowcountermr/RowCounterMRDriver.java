package com.github.minyk.rowcountermr;

import com.github.minyk.rowcountermr.mapper.DoNothingMapper;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class RowCounterMRDriver extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowCounterMRDriver.class);
    Job job;
    // Make Job obj.

    private Options buildOption() {
        Options opts = new Options();

        Option input = new Option("i", "input", true, "input location. Required.");
        input.setRequired(true);
        opts.addOption(input);

        Option counters = new Option("c", "save-counters", true, "Local path to save counters.");
        counters.setRequired(true);
        opts.addOption(counters);

        Option print = new Option("p", "print", false, "Print Counters to stdout.");
        print.setRequired(false);
        opts.addOption(print);

        return opts;
    }

    @Override
    public int run(String[] args) throws Exception {

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(buildOption(), args, true);

        Configuration config = this.getConf();

        config.set(RowCounterMRConfig.INPUT_PATH, cmd.getOptionValue('i'));
        config.set(RowCounterMRConfig.COUNTER_PATH, cmd.getOptionValue('c'));
        if(cmd.hasOption('p')) {
            config.set(RowCounterMRConfig.PRINT_COUNTERS, String.valueOf(Boolean.TRUE));
        }

        // Do the Job.
        int result = dojob(config);

        if(result == 0) {
            saveJobLogs(job);
        }

        return result;
    }

    public Job run(Configuration conf) throws Exception {

        // Do the Job.
        dojob(conf);

        return job;
    }

    private int dojob(Configuration config) throws Exception {
        job = Job.getInstance(config, "RowCounterMR");

        job.setJarByClass(RowCounterMRDriver.class);
        job.setMapperClass(DoNothingMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        Path inputPath = new Path(config.get(RowCounterMRConfig.INPUT_PATH));
        Path outputPath = new Path("/tmp/rowcounter");

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        try {
            if(fs.exists(outputPath)) {
                fs.delete(outputPath, true);
//                LOGGER.info("Existing output path deleted: " + outputPath.toUri().getPath());
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw e;
        } finally {
            fs.close();
        }

        int result = job.waitForCompletion(true) ? 0 : 1;

        return result;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new RowCounterMRDriver(), args);
        } catch (MissingOptionException e) {
            e.getMessage();
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void saveJobLogs(Job job) throws Exception {


        StringBuilder sb = new StringBuilder();

        sb.append("------------------------------------------------\n");
        sb.append("Job ends at ");
        sb.append(DateTime.now().toString() + "\n");
        //bw.write(content);

        sb.append("Job Name: " + job.getJobName() + "\n");
        // Job ID cannot obtain from job object. See https://issues.apache.org/jira/browse/MAPREDUCE-118
        //bw.write("Job Id:" + job.getJobID().toString());
        sb.append("Input Path: " + job.getConfiguration().get(RowCounterMRConfig.INPUT_PATH) + "\n");

        if(job.isSuccessful()) {
            sb.append("Job Status: Successful.\n");
            Counters counters = job.getCounters();

            sb.append("Input Records: " + counters.findCounter(Task.Counter.MAP_INPUT_RECORDS).getValue());
            sb.append("\n");
            sb.append("Read Bytes: " + counters.findCounter(FileInputFormat.Counter.BYTES_READ).getValue());
            sb.append("\n");
        } else {
            sb.append("Job Status: failed." + "\n");
        }

        String logPath = job.getConfiguration().get(RowCounterMRConfig.COUNTER_PATH);
        String filename = "RowCounterMR-" + DateTime.now().toString() + ".log";

        File file = new File(logPath + "/" + filename);

        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
        BufferedWriter bw = new BufferedWriter(fw);

        bw.write(sb.toString());

        bw.close();

        if(job.getConfiguration().getBoolean(RowCounterMRConfig.PRINT_COUNTERS,RowCounterMRConfig.DEFAULT_PRINT_COUNTERS)) {
            System.out.print(sb.toString());
        }
    }
}
