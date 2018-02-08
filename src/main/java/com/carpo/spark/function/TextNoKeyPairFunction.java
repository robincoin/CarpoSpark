package com.carpo.spark.function;

import com.carpo.spark.utils.StringsUtils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * 特定格式的数据输出
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public class TextNoKeyPairFunction implements PairFunction<String, String, String> {
    private int key_col;
    private String split;

    public TextNoKeyPairFunction(final int key_col, final String split) {
        this.key_col = key_col;
        this.split = StringsUtils.trimNull(split, ",");
    }

    @Override
    public Tuple2<String, String> call(String s) throws Exception {
        String[] data = s.split(split);
        return new Tuple2<String, String>("", s);
    }
}
