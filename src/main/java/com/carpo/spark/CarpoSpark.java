package com.carpo.spark;

import com.carpo.spark.bean.*;
import com.carpo.spark.conf.SQLConn;
import com.carpo.spark.function.KpiFunction;
import com.carpo.spark.function.TextFunction;
import com.carpo.spark.function.TextYesKeyPairFunction;
import com.carpo.spark.output.RDDMultipleTextOutputFormat;
import com.carpo.spark.utils.LogUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.*;
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
                rddStack.push(rdd);
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
                rddStack.push(rdd);
            } else if (ENodeType.distinct.name().equals(nodes.getType())) {//去重
                JavaRDDLike tempRdd = rddStack.pop();
                if (tempRdd instanceof JavaPairRDD) {
                    JavaPairRDD<String, KpiBean> rdd = ((JavaPairRDD<String, KpiBean>) tempRdd).distinct();
                    rddStack.push(rdd);
                } else if (tempRdd instanceof JavaRDD) {
                    JavaRDD<String> rdd = ((JavaRDD<String>) tempRdd).distinct();
                    rddStack.push(rdd);
                }
            } else if (ENodeType.map.name().equals(nodes.getType())) {//去重
                JavaRDD<String> tempRdd = (JavaRDD) rddStack.pop();
                JavaPairRDD<String, KpiBean> rdd = null;
                if (nodes.getKey_col() >= 0) {
                    rdd = tempRdd.mapToPair(new TextYesKeyPairFunction(nodes.getKey_col(), nodes.getValue_cols(), nodes.getSplit()));
                } else {
                    rdd = tempRdd.mapToPair(s -> new Tuple2<String, KpiBean>("", new KpiBean(s)));
                }
                rddStack.push(rdd);
            } else if (ENodeType.group.name().equals(nodes.getType())) {//计算
                JavaPairRDD<String, KpiBean> rdd = (JavaPairRDD) rddStack.pop();
                JavaPairRDD<String, KpiBean> newRDD = rdd.reduceByKey(new KpiFunction(nodes.getFields(), nodes.getSplit()));
                rddStack.push(newRDD);
            } else if (ENodeType.join.name().equals(nodes.getType())) {//join，必须有主键
                JavaPairRDD<String, KpiBean> tempRdd1 = (JavaPairRDD) rddStack.pop();
                JavaPairRDD<String, KpiBean> tempRdd2 = (JavaPairRDD) rddStack.pop();
                JavaPairRDD<String, Tuple2<KpiBean, Optional<KpiBean>>> newRDD = tempRdd1.leftOuterJoin(tempRdd2);
                newRDD.foreach(tuple2 -> {
                    System.out.println("rdd2:" + tuple2._1 + "=" + tuple2._2);
                });
                rddStack.push(newRDD);
            } else if (ENodeType.union.name().equals(nodes.getType())) {//union，列必须相同
                JavaRDDLike tempRdd = rddStack.pop();
                if (tempRdd instanceof JavaRDD) {
                    JavaRDD<String> newRdd = ((JavaRDD<String>) tempRdd).union((JavaRDD<String>) rddStack.pop());
                    rddStack.push(newRdd);
                } else {
                    JavaPairRDD<String, KpiBean> newRdd = ((JavaPairRDD<String, KpiBean>) tempRdd).union((JavaPairRDD<String, KpiBean>) rddStack.pop());
                    rddStack.push(newRdd);
                }
            } else if (ENodeType.output.name().equals(nodes.getType())) {//数据输出
                JavaPairRDD<String, KpiBean> rdd = (JavaPairRDD<String, KpiBean>) rddStack.pop();
                jobConf.setOutputFormat(RDDMultipleTextOutputFormat.class);
                RDDMultipleTextOutputFormat.setOutputPath(jobConf, new Path(carpoTask.getOutput()));
                rdd.saveAsHadoopDataset(jobConf);
                rdd.saveAsHadoopFile("/ABCDEFG", String.class, Object.class, RDDMultipleTextOutputFormat.class);
            }
        }
        sc.close();
    }
}
