package com.yjx.day02;

import com.yjx.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Hello03SourceFromKafka {
    public static void main(String[] args) throws Exception{

        //创建一个线程持续向Kafka添加数据
        new Thread(()->{
            for (int i=0 ;i<100;i++){
                KafkaUtil.sendMsg("baidu","Hello Flike Kafka"+ i+","+System.currentTimeMillis());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //1、Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setTopics("baidu")
                .setGroupId("yjxxt_ll")
                .setStartingOffsets(OffsetsInitializer.earliest())//从最后一位数据开始消费数据
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = environment.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaSource.print();
        //2、Teansformation+3.Sink
        //运行环境
        environment.execute();
    }
}
