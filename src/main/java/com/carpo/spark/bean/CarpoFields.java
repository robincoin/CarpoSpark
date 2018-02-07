package com.carpo.spark.bean;

/**
 * 文件字段相应信息
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/3
 */
public class CarpoFields {
    private int idx;//下标未知
    private String name;//文件字段,自动生成
    private String text;//中文名称
    private String oper;//分组操作

    public String getOper() {
        return oper;
    }

    public void setOper(String oper) {
        this.oper = oper;
    }

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
