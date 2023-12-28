package com.yjx.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class AggregateFunction4Order2WeightPrice extends AggregateFunction<Double, Tuple2<Integer, Integer>> {

    @Override //初始化累加器
    public Tuple2<Integer, Integer> createAccumulator() {
        //Tuple2.of(总重量，总销售额)
        return Tuple2.of(0, 0);
    }

    @FunctionHint(
            input = {@DataTypeHint("INT"), @DataTypeHint("INT")}
    )
    //数据累加
    public void accumulate(Tuple2<Integer, Integer> acc, int weight, int price) {
        acc.f0 += weight * price;
        acc.f1 += weight;
    }

    @Override//获取累加器中的结果，并进行计算
    public Double getValue(Tuple2<Integer, Integer> accumulator) {
        if (accumulator.f1 == 0) {
            return 0.0;
        }
        return accumulator.f1 * 1.0 / accumulator.f0;
    }
}
