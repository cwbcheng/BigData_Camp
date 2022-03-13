package com.github.cwbcheng.mapreducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TrafficSum {

    public static class TrafficMapper
            extends Mapper<Object, Text, Text, Traffic> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Traffic traffic = new Traffic();
            String line = value.toString();
            String[] r = line.split("\\t");
            traffic.setUpload(Long.valueOf(r[8]));
            traffic.setDownload(Long.valueOf(r[9]));
            context.write(new Text(r[1]), traffic);
        }
    }

    public static class TrafficReducer
            extends Reducer<Text, Traffic, Text, Traffic> {
        public void reduce(Text key, Iterable<Traffic> values,
                           Context context
        ) throws IOException, InterruptedException {
            long totalUpload = 0;
            long totalDownload = 0;
            for (Traffic traffic :
                    values) {
                totalUpload += traffic.getUpload();
                totalDownload += traffic.getDownload();
            }
            Traffic value = new Traffic();
            value.setUpload(totalUpload);
            value.setDownload(totalDownload);
            context.write(key, value);
        }
    }
}
