package com.yjx.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountByDataSetUseJava {
    public static void main(String[] args) throws Exception {
        //创建环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        //获取数据源读取文件
        DataSource<String> source = environment.readTextFile("data/wordcount.txt");


        /*
         * hello01 yjxxt01
         * hello02 yjxxt02
         * hello03 yjxxt03
         * hello04 yjxxt04
         * hello05 yjxxt05
         * hello06 yjxxt06
         * hello07 yjxxt07
         * hello08 yjxxt08
         * */

        //开始转换(将数据打平，拆分)
        FlatMapOperator<String, String> flatMap = source.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                //方法获取的是数据文件的每一行数据
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                    //使用collector接收转换后的数据，方便交由后续使用
                }//下方为简写，直接使用lambd表达式

//                Arrays.stream(line.split(" ")).forEach(word -> collector.collect(word));
//                Arrays.stream(line.split(" ")).forEach(collector::collect);
            }

        });

//        flatMap.print();

//        System.out.println("==========================");
        //开始计数
        MapOperator<String, Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
                //使用元组为每一个单词后附加一个参数1
            }
        });
//        map.print();
//        System.out.println("=============================");
        //开始分类并统计
//        AggregateOperator<Tuple2<String, Integer>> sum = map.sum(1);//这是直接求总和
        AggregateOperator<Tuple2<String, Integer>> sum = map.groupBy(0).sum(1);//按照元组的一个个数进行分组聚合第二个数

        //打印结果
        sum.print();

    }


}
