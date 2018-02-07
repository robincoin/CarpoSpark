package com.carpo.spark.output;

import com.carpo.spark.utils.DateUtils;
import com.carpo.spark.utils.NumberUtils;
import com.carpo.spark.utils.StringsUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * 自定义数据格式
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/3
 */
public class RDDMultipleTextOutputFormat extends
        MultipleTextOutputFormat<String, String> {
    private TextOutputFormat<String, String> theTextOutputFormat = null;
    private int fileIdx = 1;
    private JobConf jobConf;
    private String postfix;//前缀
    private String suffix;//后缀
    private String extension;//扩展名
    private int time_col;//是否有时间字段
    private String time_format1;//输入时间格式
    private String time_format2;//输出时间格式
    private static long defSize = 100 * 1024 * 1024;/*默认100M,文件大小拆分*/

    public RecordWriter<String, String> getRecordWriter(final FileSystem fs, final JobConf job, String name, final Progressable arg3) throws IOException {
        final String myName = this.generateLeafFileName(name);
        this.jobConf = job;
        this.extension = job.get("extension");
        this.postfix = job.get("postfix");
        this.suffix = job.get("suffix");
        this.time_col = NumberUtils.toInt(job.get("time_col"), -1);
        this.time_format1 = job.get("time_format1");
        this.time_format2 = job.get("time_format2");
        final long fileSize = NumberUtils.toLong(job.get("size"), defSize);
        return new RecordWriter<String, String>() {
            TreeMap<String, RecordWriter<String, String>> recordWriters = new TreeMap();

            public void write(String key, String value) throws IOException {
                String keyBasedPath = RDDMultipleTextOutputFormat.this.generateFileNameForKeyValue(key, value, myName);
                String finalPath = RDDMultipleTextOutputFormat.this.getInputFileBasedOutputFileName(job, keyBasedPath);
                Object actualKey = RDDMultipleTextOutputFormat.this.generateActualKey(key, value);
                Object actualValue = RDDMultipleTextOutputFormat.this.generateActualValue(key, value);
                RDDTextOutputFormat.RDDLineRecordWriter rw = (RDDTextOutputFormat.RDDLineRecordWriter) this.recordWriters.get(finalPath);
                if (rw == null) {
                    rw = (RDDTextOutputFormat.RDDLineRecordWriter) RDDMultipleTextOutputFormat.this.getBaseRecordWriter(fs, job, finalPath, arg3);
                    this.recordWriters.put(finalPath, rw);
                }
                rw.write(actualKey, actualValue);
                if (rw.getSize() > fileSize) {
                    fileIdx++;
                }
            }

            public void close(Reporter reporter) throws IOException {
                Iterator keys = this.recordWriters.keySet().iterator();
                while (keys.hasNext()) {
                    RecordWriter rw = (RecordWriter) this.recordWriters.get(keys.next());
                    rw.close(reporter);
                }
                this.recordWriters.clear();
            }
        };
    }

    @Override
    protected String generateFileNameForKeyValue(String key, String value, String name) {
        final StringBuilder sb = new StringBuilder();
        if (StringsUtils.isNotEmpty(postfix))
            sb.append(postfix).append("_");
        if (time_col != -1) {
            if (time_format1 == null || time_format1.equals(time_format2)) {
                sb.append(value.split(",")[time_col]);
            } else {
                sb.append(DateUtils.formatDate(value.split(",")[time_col], time_format1, time_format2));
            }
            sb.append("_");
        }
        sb.append(fileIdx).append("_").append(name.split("-")[1]);
        if (StringsUtils.isNotEmpty(suffix))
            sb.append("_").append(suffix);
        sb.append(".").append(extension);
        return sb.toString();
    }

    protected RecordWriter<String, String> getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3) throws IOException {
        if (this.theTextOutputFormat == null) {
            this.theTextOutputFormat = new RDDTextOutputFormat();
        }
        return this.theTextOutputFormat.getRecordWriter(fs, job, name, arg3);
    }

    /**
     * 文件名路径
     *
     * @param job
     * @param name
     * @return
     */
    protected String getInputFileBasedOutputFileName(JobConf job, String name) {
        String infilepath = job.get("mapreduce.map.input.file");
        if (infilepath == null) {
            return name;
        } else {
            int numOfTrailingLegsToUse = job.getInt("mapred.outputformat.numOfTrailingLegs", 0);
            if (numOfTrailingLegsToUse <= 0) {
                return name;
            } else {
                Path infile = new Path(infilepath);
                Path parent = infile.getParent();
                String midName = infile.getName();
                Path outPath = new Path(midName);

                for (int i = 1; i < numOfTrailingLegsToUse && parent != null; ++i) {
                    midName = parent.getName();
                    if (midName.length() == 0) {
                        break;
                    }
                    parent = parent.getParent();
                    outPath = new Path(midName, outPath);
                }
                return outPath.toString();
            }
        }
    }

    /**
     * 判断文件路径是否存在
     *
     * @param ignored
     * @param job
     * @throws InvalidJobConfException
     * @throws IOException
     */
    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job)
            throws InvalidJobConfException,
            IOException {
        Path outDir = getOutputPath(job);
        if (outDir == null && job.getNumReduceTasks() != 0) {
            throw new InvalidJobConfException(
                    "Output directory not set in JobConf.");
        }
        if (outDir != null) {
            FileSystem fs = outDir.getFileSystem(job);
            // normalize the output directory
            outDir = fs.makeQualified(outDir);
            setOutputPath(job, outDir);
            // get delegation token for the outDir's file system
            TokenCache.obtainTokensForNamenodes(job.getCredentials(),
                    new Path[]{outDir}, job);
            //使spark的输出目录可以存在
            // check its existence
            /*if (fs.exists(outDir)) {
                throw new FileAlreadyExistsException("Output directory "
                        + outDir + " already exists");
            }*/
        }
    }
}