package com.carpo.spark;

import com.carpo.spark.bean.CarpoFlowChart;
import com.carpo.spark.bean.CarpoNodes;
import com.carpo.spark.bean.CarpoTask;
import com.carpo.spark.bean.ENodeType;
import com.carpo.spark.conf.SQLConn;
import com.carpo.spark.function.TextFunction;
import com.carpo.spark.function.TextPairFunction;
import com.carpo.spark.output.RDDMultipleTextOutputFormat;
import com.carpo.spark.utils.LogUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Stack;


/**
 * 定制化Spark流程图,启动程序
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2017/12/5
 */
public class CarpoSpark {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            LogUtils.info("sparkId is not null");
            return;
        }

        LogUtils.info("will load carpo data :" + args[0]);
        CarpoTask carpoTask = SQLConn.fromJson(args[0]);
        if (carpoTask == null) {
            LogUtils.info("can not find " + args[0]);
            return;
        }

        CarpoFlowChart flowChart = carpoTask.getFlowChart();
        if (!flowChart.isValid()) {
            LogUtils.info("FlowChart is not valid");
            return;
        }

        SparkSession spark = SparkSession
                .builder()
                .appName(carpoTask.getName())
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        //通用常量，上下文传递参数
        JobConf jobConf = carpoTask.getJobConf();
        //获取节点执行顺序
        Stack<CarpoNodes> nodeStack = flowChart.getExeOrder();
        //把RDD压入栈里面
        Stack<JavaRDDLike> rddStack = new Stack<>();
        LogUtils.info("task split :" + carpoTask.getSplit());
        while (nodeStack.size() > 0) {
            CarpoNodes nodes = nodeStack.pop();
            nodes.initJobConf(jobConf);
            LogUtils.info("node is :" + nodes.getId());
            LogUtils.info("node split :" + nodes.getSplit());
            if (ENodeType.input.name().equals(nodes.getType())) {//数据源RDD
                JavaRDD<String> rdd = sc.textFile(nodes.getInput());
                rddStack.push(rdd);
            } else if (ENodeType.filter_col.name().equals(nodes.getType())) {//过滤列
                JavaRDD<String> rdd = rddStack.pop().map(new TextFunction(nodes.getCols(), nodes.getSplit()));
                rddStack.add(rdd);
            } else if (ENodeType.filter_row.name().equals(nodes.getType())) {//过滤行
                JavaRDD<String> rdd = ((JavaRDD<String>) rddStack.pop()).filter(s -> {
                    try {
                        String[] values = s.split(nodes.getSplit());
                        return !values[nodes.getFilter_key()].contains(nodes.getFilter_value());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return true;
                });
                rddStack.add(rdd);
            } else if (ENodeType.distinct.name().equals(nodes.getType())) {//去重
                JavaRDDLike tempRdd = rddStack.pop();
                if (tempRdd instanceof JavaPairRDD) {
                    JavaPairRDD<String, String> rdd = ((JavaPairRDD<String, String>) tempRdd).distinct();
                    rddStack.add(rdd);
                } else if (tempRdd instanceof JavaRDD) {
                    JavaRDD<String> rdd = ((JavaRDD<String>) tempRdd).distinct();
                    rddStack.add(rdd);
                }
            } else if (ENodeType.join.name().equals(nodes.getType())) {//join，必须有主键
                JavaPairRDD<String, String> rdd = (JavaPairRDD<String, String>) rddStack.pop();
                JavaPairRDD<String, Tuple2<String, org.apache.spark.api.java.Optional<String>>> newRdd = rdd.leftOuterJoin((JavaPairRDD<String, String>) rddStack.pop());
            } else if (ENodeType.union.name().equals(nodes.getType())) {//union，列必须相同
                JavaRDDLike tempRdd = (JavaRDD<String>) rddStack.pop();
                if (tempRdd instanceof JavaRDD) {
                    JavaRDD<String> newRdd = ((JavaRDD<String>) tempRdd).union((JavaRDD<String>) rddStack.pop());
                    rddStack.add(newRdd);
                } else {
                    JavaPairRDD<String, String> newRdd = ((JavaPairRDD<String, String>) tempRdd).union((JavaPairRDD<String, String>) rddStack.pop());
                    rddStack.add(newRdd);
                }
            } else if (ENodeType.output.name().equals(nodes.getType())) {//数据输出
                JavaRDDLike tempRdd = rddStack.pop();
                JavaPairRDD<String, String> rdd = null;
                if (tempRdd instanceof JavaRDD) {
                    rdd = ((JavaRDD<String>) tempRdd).mapToPair(new TextPairFunction(nodes.getCols(), nodes.getSplit(), carpoTask.getSplit()));
                } else {
                    rdd = (JavaPairRDD<String, String>) tempRdd;
                }
                jobConf.setOutputFormat(RDDMultipleTextOutputFormat.class);
                RDDMultipleTextOutputFormat.setOutputPath(jobConf, new Path(carpoTask.getOutput()));
                rdd.saveAsHadoopDataset(jobConf);
                rdd.saveAsHadoopFile("/ABCDEFG", String.class, String.class, RDDMultipleTextOutputFormat.class);
            }
        }
        sc.close();
    }
}
