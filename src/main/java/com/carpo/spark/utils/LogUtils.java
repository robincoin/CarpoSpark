package com.carpo.spark.utils;

import java.time.LocalDateTime;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/4
 */
public class LogUtils {
    /**
     * 正常信息
     * @param obj
     */
    public static void info(Object obj) {
        System.out.println(LocalDateTime.now().toString() + ":" + obj);
    }

    /**
     * 错误日志
     * @param e
     */
    public static void error(Throwable e) {
        System.out.println(LocalDateTime.now().toString() + ":" + e.getMessage());
        e.printStackTrace();
    }
}
