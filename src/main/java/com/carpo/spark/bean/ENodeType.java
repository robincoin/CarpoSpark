package com.carpo.spark.bean;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public enum ENodeType {
    input("HDFS", ENodeType.filter_col, ENodeType.filter_row, ENodeType.distinct, ENodeType.map, ENodeType.union),
    filter_col("过滤列", ENodeType.distinct, ENodeType.union, ENodeType.map, ENodeType.filter_row),
    filter_row("过滤行", ENodeType.distinct, ENodeType.union, ENodeType.map, ENodeType.filter_col),
    distinct("去重", ENodeType.group, ENodeType.join, ENodeType.union, ENodeType.map, ENodeType.output),
    map("Key Value", ENodeType.group, ENodeType.join, ENodeType.distinct, ENodeType.output),
    join("合并", ENodeType.distinct, ENodeType.group, ENodeType.output),
    union("联合", ENodeType.distinct, ENodeType.group, ENodeType.output),
    group("分组", ENodeType.output),
    output("HDFS");
    public String name;
    public ENodeType[] nexts;

    private ENodeType(String name, ENodeType... nexts) {
        this.name = name;
        this.nexts = nexts;
    }
}
