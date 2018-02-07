package com.carpo.spark.bean;

import com.carpo.spark.utils.IOUtils;
import com.google.gson.Gson;

import java.io.File;
import java.util.Stack;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public class Main {
    public static void main(String[] args) {
        Gson gson = new Gson();
        String json = IOUtils.readFromFileAsString(new File("E:\\train\\job1.json"));
        CarpoTask task = gson.fromJson(json, CarpoTask.class);
        CarpoFlowChart chart = task.getFlowChart();
        Stack<CarpoNodes> stack = chart.getExeOrder();
        System.out.println(stack);
    }
}
