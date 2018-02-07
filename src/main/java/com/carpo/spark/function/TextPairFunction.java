package com.carpo.spark.function;

import com.carpo.spark.utils.StringsUtils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 特定格式的数据输出
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public class TextPairFunction implements PairFunction<String, String, String> {
    private List<Integer> cols;
    private String split_in;
    private String split_out;

    public TextPairFunction(final List<Integer> cols, final String split_in, final String split_out) {
        this.cols = cols;
        this.split_in = StringsUtils.trimNull(split_in, ",");
        this.split_out = StringsUtils.trimNull(split_out, ",");
    }

    @Override
    public Tuple2<String, String> call(String s) throws Exception {
        String[] datas = s.split(split_in);
        String[] newDatas = new String[cols.size()];
        for (int idx : cols) {
            newDatas[idx] = datas[idx];
        }
        return new Tuple2<String, String>("", StringsUtils.joinArray(newDatas, split_out));
    }
}
