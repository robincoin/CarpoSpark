package com.carpo.spark.bean;

import com.carpo.spark.utils.StringsUtils;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 执行RDD节点，例如读取文件，过滤，去重，Union
 *
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/3
 */
public class CarpoNodes {
    private String id;//本身Id
    private String refId;//依赖Id
    private int childs;//子节点数量
    private String input;//HDFS路径或者其他
    private String type;//ENodeType
    private String split;//分隔符
    private String key_col;//主键
    private String time_col;//时间字段
    private String time_format1;//时间格式输入
    private String time_format2;//时间格式输出
    private Map<String, CarpoFields> fields;//字段
    private int filter_key;//行过滤指定字段
    private String filter_value;//行过滤指定字段对应值

    public int getFilter_key() {
        return filter_key;
    }

    public void setFilter_key(int filter_key) {
        this.filter_key = filter_key;
    }

    public String getFilter_value() {
        return filter_value;
    }

    public void setFilter_value(String filter_value) {
        this.filter_value = filter_value;
    }

    public void setSplit(String split) {
        this.split = split;
    }

    public String getSplit() {
        return split;
    }

    public void setChilds(int childs) {
        this.childs = childs;
    }

    public int getChilds() {
        return childs;
    }

    public String getRefId() {
        return refId;
    }

    public CarpoNodes setRefId(String refId) {
        this.refId = refId;
        return this;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getKey_col() {
        return key_col;
    }

    public void setKey_col(String key_col) {
        this.key_col = key_col;
    }

    public String getTime_col() {
        return time_col;
    }

    public void setTime_col(String time_col) {
        this.time_col = time_col;
    }

    public Map<String, CarpoFields> getFields() {
        return fields;
    }

    public void setFields(Map<String, CarpoFields> fields) {
        this.fields = fields;
    }

    public String getTime_format1() {
        return time_format1;
    }

    public void setTime_format1(String time_format1) {
        this.time_format1 = time_format1;
    }

    public String getTime_format2() {
        return time_format2;
    }

    public void setTime_format2(String time_format2) {
        this.time_format2 = time_format2;
    }

    public void initJobConf(JobConf conf) {
        if (StringsUtils.isNotEmpty(time_col)) {
            conf.set("time_col", time_col);
            if (StringsUtils.isNotEmpty(time_format1)) {
                conf.set("time_format1", time_format1);
                conf.set("time_format2", time_format2);
            }
        }
    }

    public List<Integer> getCols() {
        return fields.values().stream().map(f -> f.getIdx()).collect(Collectors.toList());
    }
}
