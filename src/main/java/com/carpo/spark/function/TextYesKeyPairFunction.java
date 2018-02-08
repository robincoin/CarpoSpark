package com.carpo.spark.function;

import com.carpo.spark.bean.KpiBean;
import com.carpo.spark.utils.StringsUtils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * 特定格式的数据输出
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public class TextYesKeyPairFunction implements PairFunction<String, String, KpiBean> {
    private int key_col;
    private int[] value_cols;
    private String split;

    public TextYesKeyPairFunction(final int key_col, int[] value_cols, final String split) {
        this.key_col = key_col;
        this.value_cols = value_cols;
        this.split = StringsUtils.trimNull(split, ",");
    }

    @Override
    public Tuple2<String, KpiBean> call(String s) throws Exception {
        String[] data = s.split(split);
        if (value_cols != null) {
            s = "";
            for (int idx : value_cols) {
                s += data[idx] + ",";
            }
            s.substring(0, s.length() - 1);
        }
        return new Tuple2<String, KpiBean>(data[key_col], new KpiBean(s));
    }
}
