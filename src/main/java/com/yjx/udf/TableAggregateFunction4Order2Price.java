package com.yjx.udf;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class TableAggregateFunction4Order2Price extends TableAggregateFunction<String, Tuple3<Integer, Integer, Boolean>> {
    @Override
    public Tuple3<Integer, Integer, Boolean> createAccumulator() {
        //Tuple3.of(最高价格,第二高价格,是否发生改变)
        return Tuple3.of(0, 0, false);
    }

    public void accumulate(Tuple3<Integer, Integer, Boolean> acc, Integer price) {
        if (price > acc.f0) {
            acc.f2 = true;
            acc.f1 = acc.f0;
            acc.f0 = price;
        } else if (price > acc.f1) {
            acc.f2 = true;
            acc.f1 = price;
        } else {
            acc.f2 = false;
        }
    }

    public void emitValue(Tuple3<Integer, Integer, Boolean> acc, Collector<String> out) {
        // emit the value and rank
        if (acc.f2) {
            acc.f2 = false;
            out.collect("First[" + acc.f0 + "]Second[" + acc.f1 + "]");
        }
    }
}
