package com.carpo.spark.conf;

import com.google.gson.Gson;
import com.carpo.spark.bean.CarpoTask;

import java.sql.*;
import java.util.Properties;

/**
 * 从数据库获取指定的执行Carpo任务
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/3
 */
public class SQLConn {
    static Properties pro = null;

    public SQLConn() {
        try {
            if (pro == null) {
                pro = new Properties();
                pro.load(SQLConn.class.getResourceAsStream("mysql.properties"));
                Class.forName((String) pro.get("driver"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取数据库连接
     *
     * @return
     */
    public Connection getConnection() {
        try {
            return DriverManager.getConnection((String) pro.get("url"), (String) pro.get("username"), (String) pro.get("password"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 关闭数据库连接
     *
     * @param closeable
     */
    public void closeSQl(AutoCloseable... closeable) {
        if (closeable != null) {
            for (AutoCloseable close : closeable) {
                try {
                    close.close();
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * 根据ID加载Carpo数据，并进行对象化
     *
     * @param id
     * @return
     */
    public static CarpoTask fromJson(String id) {
        SQLConn sqlConn = new SQLConn();
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            conn = sqlConn.getConnection();
            st = conn.createStatement();
            rs = st.executeQuery("select * from carpo_spark_task where id='" + id + "'");
            while (rs.next()) {
                Gson gson = new Gson();
                return gson.fromJson(rs.getString("value"), CarpoTask.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sqlConn.closeSQl(conn, st, rs);
        }
        return null;
    }
}
