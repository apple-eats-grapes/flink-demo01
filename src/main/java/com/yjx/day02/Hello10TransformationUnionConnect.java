package com.yjx.day02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;


/*
* connect 对两个流中的数据进行筛选加工
*
*
* */
public class Hello10TransformationUnionConnect {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

        DataStreamSource<String> source1 = environment.fromElements("yes", "no", "no", "yes");
        DataStreamSource<Double> source2 = environment.fromElements(33.1, 33.6, 50.2, 38.9);

        ConnectedStreams<String, Double> connect = source1.connect(source2);

        connect.map(new CoMapFunction<String, Double, String >() {
            @Override
            public String map1(String value) throws Exception {
                if ("yes".equals(value)){
                    return "["+value +"]火警传感器【安全】";
                }
                return "[" +value+ "]火警传感器【危险】";
            }

            @Override
            public String map2(Double value) throws Exception {
                if (value<=50){
                    return "["+value+"]温度传感器【安全】";
                }
                return "["+value+"]温度传感器【危险】";
            }
        }).print();

        environment.execute();


    }
}
