package com.carpo.spark.bean;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public enum ENodeType {
    input("HDFS", "filter_col", "filter_row", "distinct", "map", "union"),
    filter_col("过滤列", "distinct", "union", "map", "filter_row"),
    filter_row("过滤行", "distinct", "union", "map", "filter_col"),
    distinct("去重", "group", "join", "union", "map", "output"),
    map("Key Value", "group", "join", "distinct", "output"),
    join("合并", "distinct", "group", "output"),
    union("联合", "distinct", "group", "output"),
    group("分组", "output"),
    output("HDFS");
    public String name;
    public String[] nexts;

    private ENodeType(String name, String... nexts) {
        this.name = name;
        this.nexts = nexts;
    }
}
