package com.yjx.day04.flink;


import com.yjx.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello07CoGroup {
    public static void main(String[] args) throws Exception {

        //创建一个线程生成数据
        new Thread(() -> {
            for (int i = 100; i < 200; i++) {
                //生成一个商品ID
                String goodId = RandomStringUtils.randomAlphabetic(16).toLowerCase();
                //发送goodInfo数据 [id:info:ts]
                KafkaUtil.sendMsg("t_goodinfo", goodId + ":info" + i + ":" + System.currentTimeMillis());
                if (i % 5 != 0) {
                    //创建goodPrice数据[id:price:ts]
                    KafkaUtil.sendMsg("t_goodprice", goodId + ":" + i + ":" + System.currentTimeMillis());
                }
                //让线程休眠一下
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        //获取数据源
        DataStreamSource<String> goodInfoSource = environment.fromSource(KafkaUtil.getKafkaSource("t_goodinfo", "liyi"), WatermarkStrategy.noWatermarks(), "Kafka Source Info");
        DataStreamSource<String> goodPriceSource = environment.fromSource(KafkaUtil.getKafkaSource("t_goodprice", "liyi"), WatermarkStrategy.noWatermarks(), "Kafka Source Price");
        //添加水位线
        SingleOutputStreamOperator<Tuple3<String, String, Long>> infoStream = goodInfoSource.map(record -> {
                    String[] split = record.split(":");
                    return Tuple3.of(split[0], split[1], Long.parseLong(split[2]));
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTime) -> {
                            return element.f2;
                        }));
        SingleOutputStreamOperator<Tuple3<String, String, Long>> priceStream = goodPriceSource.map(record -> {
                    String[] split = record.split(":");
                    return Tuple3.of(split[0], split[1], Long.parseLong(split[2]));
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTime) -> {
                            return element.f2;
                        }));
        //开始进行流的Join
        infoStream.coGroup(priceStream)
                .where(i -> i.f0)
                .equalTo(p -> p.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Long>> info, Iterable<Tuple3<String, String, Long>> price, Collector<String> out) throws Exception {
                        String s = RandomStringUtils.randomAlphabetic(8);
                        //收集Info
                        for (Tuple3<String, String, Long> tuple3 : info) {
                            out.collect(s + "--" + tuple3.toString());
                        }
                        //收集Price
                        for (Tuple3<String, String, Long> tuple3 : price) {
                            out.collect(s + "--" + tuple3.toString());
                        }
                    }
                }).print("CoGroup--").setParallelism(1);

        //运行环境
        environment.execute();
    }
}
