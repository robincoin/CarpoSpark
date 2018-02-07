package com.carpo.spark.bean;

import org.apache.hadoop.mapred.JobConf;

import java.util.Map;

/**
 * 根据Json对象化的数据
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/3
 */
public class CarpoTask {
    private String id;//任务Id
    private String name;//任务名称
    private String output;//输出目录
    private String format;//文件名格式化
    private int size;//文件大小
    private String postfix;//前缀
    private String suffix;//后缀
    private String split;//输出文件分隔符
    private String extension;//扩展名
    private Map<String, CarpoNodes> nodes;//结点
    private Map<String, CarpoLines> lines;//走向

    public void setSplit(String split) {
        this.split = split;
    }

    public String getSplit() {
        return split;
    }

    public Map<String, CarpoLines> getLines() {
        return lines;
    }

    public void setLines(Map<String, CarpoLines> lines) {
        this.lines = lines;
    }

    public Map<String, CarpoNodes> getNodes() {
        return nodes;
    }

    public void setNodes(Map<String, CarpoNodes> nodes) {
        this.nodes = nodes;
    }

    public String getPostfix() {
        return postfix;
    }

    public void setPostfix(String postfix) {
        this.postfix = postfix;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public String getExtension() {
        return extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public JobConf getJobConf() {
        final JobConf jobConf = new JobConf();
        jobConf.set("extension", extension);
        jobConf.set("postfix", postfix);
        jobConf.set("suffix", suffix);
        jobConf.set("size", String.valueOf(size * 1024));
        return jobConf;
    }

    public CarpoFlowChart getFlowChart() {
        return new CarpoFlowChart(this);
    }
}
