package com.yjx.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/*
* sink 类算子
*
* */
public class Hello11SinkCommon {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        //操作数据
        DataStreamSource<Integer> source = environment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);

        //输出到控制台
//        source.print("输出到控制台");
        //写入文件
//        source.writeAsText("data/text_"+System.currentTimeMillis());


        environment.execute();

    }
}
