package com.carpo.spark.bean;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public enum EOperType {
    min("最小"), max("最大"), avg("平均"), sum("求和"), count("计数");
    public String name;

    private EOperType(String name) {
        this.name = name;
    }
}
