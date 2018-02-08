package com.carpo.spark.function;

import com.carpo.spark.bean.CarpoFields;
import com.carpo.spark.bean.KpiBean;
import org.apache.spark.api.java.function.Function2;

import java.util.Map;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/8
 */
public class KpiFunction implements Function2<KpiBean, KpiBean, KpiBean> {
    private Map<String, CarpoFields> groupCols;
    private String split;

    public KpiFunction(Map<String, CarpoFields> groupCols, String split) {
        this.groupCols = groupCols;
        this.split = split;
    }

    @Override
    public KpiBean call(KpiBean kpiBean, KpiBean kpiBean2) throws Exception {
        kpiBean.initKpi(groupCols,split,kpiBean2.str);
        return kpiBean;
    }
}
