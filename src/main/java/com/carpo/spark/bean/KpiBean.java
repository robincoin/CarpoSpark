package com.carpo.spark.bean;

import com.carpo.spark.utils.NumberUtils;
import com.carpo.spark.utils.StringsUtils;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/8
 */
public class KpiBean implements Serializable {
    public String str;//字符串
    public Map<String, Double> kpiMap = new LinkedHashMap<>();

    public KpiBean() {
    }

    public KpiBean(String str) {
        this.str = str;
    }

    @Override
    public String toString() {
        if (kpiMap.isEmpty())
            return str;
        return StringsUtils.join(kpiMap.values(), ",");
    }

    public String toString(String split) {
        if (kpiMap.isEmpty())
            return str;
        return StringsUtils.join(kpiMap.values(), split);
    }

    private void paserKpi(String s, String split, Map<String, CarpoFields> groupCols) {
        String[] datas = s.split(split);
        for (CarpoFields fields : groupCols.values()) {
            EOperType oper = EOperType.valueOf(fields.getOper());
            int idx = fields.getIdx();
            double value = NumberUtils.toDouble(datas[idx], 0);
            String name = fields.getName();
            double v = 0;
            switch (oper) {
                case avg:
                    v = Optional.ofNullable(kpiMap.get(name)).orElse(new Double(0));
                    v += value;
                    v /= 2;
                    kpiMap.put(name, v);
                    break;
                case sum:
                    v = Optional.ofNullable(kpiMap.get(name)).orElse(new Double(0));
                    v += value;
                    kpiMap.put(name, v);
                    break;
                case count:
                    v = Optional.ofNullable(kpiMap.get(name)).orElse(new Double(0));
                    v++;
                    kpiMap.put(name, v);
                    break;
                case min:
                    v = Optional.ofNullable(kpiMap.get(name)).orElse(new Double(Double.MAX_VALUE));
                    v = Math.min(v, value);
                    kpiMap.put(name, v);
                    break;
                case max:
                    v = Optional.ofNullable(kpiMap.get(name)).orElse(new Double(Double.MIN_VALUE));
                    v = Math.max(v, value);
                    kpiMap.put(name, v);
                    break;
            }

        }
    }

    public void initKpi(Map<String, CarpoFields> groupCols, String split, String s2) {
        if (kpiMap.isEmpty()) {
            paserKpi(str, split, groupCols);
        }
        paserKpi(s2, split, groupCols);
    }
}
