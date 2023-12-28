package com.yjx.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


public class Hello09WordCountTypes {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //source 流处理Api
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);//这里启动对应的发送接口，打开cmd后   nc -lp  端口号

      /*  SingleOutputStreamOperator<String> flatMap = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyBy = map.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });

        keyBy.sum(1).print();
        */

       /* source.flatMap((line,collector)->{
            String[] words = line.split(" ");
            for (String word:words){
                collector.collect(word);
            }
        }, Types.STRING)
                .map(word->Tuple2.of(word,1),Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                .print();*/

      /*      source.flatMap((line,collecter)->{
            Arrays.stream(line.split(" ")).forEach(collecter::collect);
        },Types.STRING)
                .map(word->Tuple2.of(word,1),Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                .print();
*/

        source.flatMap((line,collector)->{
            Arrays.stream(line.split(" ")).forEach(collector::collect);
        }, TypeInformation.of(new TypeHint<String>() {
        })).map(word->Tuple2.of(word,1),Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                .print();


        environment.execute();


    }
}
