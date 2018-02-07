package com.carpo.spark.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/3
 */
public class RDDTextOutputFormat<K, V> extends org.apache.hadoop.mapred.TextOutputFormat<K, V> {
    public RDDTextOutputFormat() {
    }

    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", "");
//        String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", "\t");
        if (!isCompressed) {
            Path codecClass1 = FileOutputFormat.getTaskOutputPath(job, name);
            FileSystem codec1 = codecClass1.getFileSystem(job);
            FSDataOutputStream file1 = codec1.create(codecClass1, progress);
            return new RDDLineRecordWriter(file1, keyValueSeparator);
        } else {
            Class codecClass = getOutputCompressorClass(job, GzipCodec.class);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);
            Path file = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(job);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new RDDLineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
        }
    }

    protected static class RDDLineRecordWriter<K, V> implements RecordWriter<K, V> {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        protected DataOutputStream out;
        private final byte[] keyValueSeparator;
        private long size = 0;//已写文件大小

        public RDDLineRecordWriter(DataOutputStream out, String keyValueSeparator) {
            this.out = out;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes("UTF-8");
            } catch (UnsupportedEncodingException var4) {
                throw new IllegalArgumentException("can\'t find UTF-8 encoding");
            }
        }

        public RDDLineRecordWriter(DataOutputStream out) {
            this(out, "\t");
        }

        public long getSize() {
            return size;
        }

        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text) o;
                int len = to.getLength();
                size += len;
                this.out.write(to.getBytes(), 0, len);
            } else {
                byte[] bs = o.toString().getBytes("UTF-8");
                size += bs.length;
                this.out.write(bs);
            }
        }

        public synchronized void write(K key, V value) throws IOException {
            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (!nullKey || !nullValue) {
                if (!nullKey) {
                    this.writeObject(key);
                }

                if (!nullKey && !nullValue) {
                    this.out.write(this.keyValueSeparator);
                }

                if (!nullValue) {
                    this.writeObject(value);
                }

                this.out.write(newline);
            }
        }

        public synchronized void close(Reporter reporter) throws IOException {
            this.out.close();
        }

        static {
            try {
                newline = "\n".getBytes("UTF-8");
            } catch (UnsupportedEncodingException var1) {
                throw new IllegalArgumentException("can\'t find UTF-8 encoding");
            }
        }
    }
}
