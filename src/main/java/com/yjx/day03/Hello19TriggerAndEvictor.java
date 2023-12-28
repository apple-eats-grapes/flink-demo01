package com.yjx.day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
/*
*//*
*
* 组件trigger、evictor的使用、以及全局窗口的应哟，该窗口包含所有元素，所以可以不用分区
* *//*
public class Hello19TriggerAndEvictor {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源-admin:3
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);
        //TimeWindow--Sliding
        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)//是否使用，根据后续是否需要分组聚合输出采用
                .window(GlobalWindows.create())//全局窗口，表示接收不同key的元素
                .trigger(CountTrigger.of(10))//满足10个元素时触发窗口计算
                .evictor(CountEvictor.of(10))//当数据再次满10个时踢出原来窗口中的数据，新来的进入窗口计算。
                .reduce((t1, t2) -> {
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                })
                .print("TimeWindow--Sliding:").setParallelism(1);

        //运行环境
        environment.execute();
    }
}*/
