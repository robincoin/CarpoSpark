package com.carpo.spark.function;

import com.carpo.spark.utils.StringsUtils;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/**
 * 特定格式的数据输出
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public class TextFunction implements Function<String, String> {
    private List<Integer> cols;
    private String split_in;

    public TextFunction(final List<Integer> cols, final String split_in) {
        this.cols = cols;
        this.split_in = StringsUtils.trimNull(split_in, ",");
    }


    @Override
    public String call(String s) throws Exception {
        String[] datas = s.split(split_in);
        String[] newDatas = new String[cols.size()];
        for (int idx : cols) {
            newDatas[idx] = datas[idx];
        }
        return StringsUtils.joinArray(newDatas, split_in);
    }
}
