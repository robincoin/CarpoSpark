package com.carpo.spark.bean;

/**
 * 各个节点的连线，根据连线获取执行顺序
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/3
 */
public class CarpoLines {
    private String inputs;//输入
    private String outputs;//输出

    public String getInputs() {
        return inputs;
    }

    public void setInputs(String inputs) {
        this.inputs = inputs;
    }

    public String getOutputs() {
        return outputs;
    }

    public void setOutputs(String outputs) {
        this.outputs = outputs;
    }
}
