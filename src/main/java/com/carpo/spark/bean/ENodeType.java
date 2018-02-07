package com.carpo.spark.bean;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public enum ENodeType {
    input("HDFS"), output("HDFS"), filter_col("过滤列"), filter_row("行"), distinct("去重"), join("合并"), union("联合"), group("分组");
    public String name;

    private ENodeType(String name) {
        this.name = name;
    }
}
