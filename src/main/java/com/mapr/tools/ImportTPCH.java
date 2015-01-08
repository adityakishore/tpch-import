package com.mapr.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mapr.tools.impl.Lineitem;
import com.mapr.tools.impl.TsvParser;
import com.mapr.tools.impl.TsvParser.ParsedLine;

public class ImportTPCH extends Configured implements Tool {

  public final static String SEPARATOR_CONF_KEY = "importtpch.separator";

  public final static String TABLE_TYPE = "importtpch.tabletype";

  public final static String PRINT_VIEW = "importtpch.printview";
  
  enum Tables {
    LINEITEM,
    ORDERS
  }

  public ImportTPCH(Configuration conf) {
    super(conf);
  }

  public int run(String[] args) throws Exception {
    if (args == null || args.length != 2) {
      System.err.println("ImportTPCH:usage");
      System.err.println("  com.mapr.tools.ImportTPCH <tablename> <inputDir>");
      System.exit(1);
    }
    String tableName = args[0];
    getTableCreator(getConf()).create(tableName);

    Path inputDir = new Path(args[1]);
    Job job = Job.getInstance(getConf());
    job.setJarByClass(getClass());
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(TPCHMapper.class);
    job.setNumReduceTasks(0);
    TableMapReduceUtil.initTableReducerJob(tableName, null, job);
    if (job.waitForCompletion(true)) {
      if (getConf().getBoolean(PRINT_VIEW, false)) {
        System.out.println(getViewBuilder(getConf()).getView(tableName+"_view", tableName));
      }
      return 0;
    } else {
      return 1;
    }
  }

  public static class TPCHMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    protected TsvParser parser;
    protected PutBuilder putBuilder;

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      ParsedLine parsed = parser.parse(value.getBytes(), value.getLength());
      Put put = putBuilder.build(parsed);
      context.write(new ImmutableBytesWritable(put.getRow()), put);
    }

    @Override
    protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      parser = new TsvParser(conf.get(SEPARATOR_CONF_KEY));
      putBuilder = getPutBuilder(conf);
    }

  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new ImportTPCH(HBaseConfiguration.create()), args));
//    TsvParser parser = new TsvParser("|");
//    byte[] lineBytes = "1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|".getBytes(Charsets.US_ASCII);
//    ParsedLine line = parser.parse(lineBytes, lineBytes.length);
//    LineitemBuilder builder = new LineitemBuilder();
//    builder.build(line);
  }

  public static TableCreator getTableCreator(Configuration conf) {
    return (TableCreator) getObject(conf);
  }

  public static PutBuilder getPutBuilder(Configuration conf) {
    return (PutBuilder) getObject(conf);
  }

  public static ViewBuilder getViewBuilder(Configuration conf) {
    return (ViewBuilder) getObject(conf);
  }

  static Map<Tables, Object> tableObjectMap = new HashMap<Tables, Object>();

  private static Object getObject(Configuration conf) {
    Object obj = null;
    String type = conf.get(TABLE_TYPE);
    try {
      Tables table = Tables.valueOf(type.toUpperCase());
      obj = tableObjectMap.get(table);
      if (obj == null) {
        synchronized (tableObjectMap) {
          obj = tableObjectMap.get(table);
          if (obj == null) {
            switch (table) {
            case LINEITEM:
              obj = new Lineitem(conf);
              break;
            default:
              throw new IllegalArgumentException("Unsupported table type!!!");
            }
            tableObjectMap.put(table, obj);
          }
        }
      }
    } catch (Throwable t) {
      throw new IllegalArgumentException("Invalid table type: " + type, t);
    }
    return obj;
  }

}
