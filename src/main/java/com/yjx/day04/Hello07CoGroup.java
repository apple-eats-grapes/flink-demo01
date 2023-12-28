package com.yjx.day04;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import com.yjx.util.KafkaUtil;

import java.time.Duration;

public class Hello07CoGroup {
    public static void main(String[] args) throws Exception {
        //创建一个线程生成器
        new Thread(()->{
            for (int i=200 ;i<300 ;i++){
                //生成一个上坪ID
                String goodId = RandomStringUtils.randomAlphabetic(16).toLowerCase();//创建16位随机大小字母字符串，并全部转成小写

                //发送goodInfo数据
                KafkaUtil.sendMsg("t_goodinfo",goodId+":info" + i + ":" + System.currentTimeMillis());

                if (i%5 !=0){
                    KafkaUtil.sendMsg("t_goodprice",goodId+":price" + i + ":" + System.currentTimeMillis());
                }//当i为5的倍数，就没有price数据
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        //创建环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> goodInfoSource = environment.fromSource(KafkaUtil.getKafkaSource("t_goodinfo", "li"), WatermarkStrategy.noWatermarks(), "Kafka Source Info");
        DataStreamSource<String> goodPriceSource = environment.fromSource(KafkaUtil.getKafkaSource("t_goodprice", "li"), WatermarkStrategy.noWatermarks(), "Kafka Source Price");

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
        //进行join（coGroup 直接将匹配的的与不匹配的数据都分别直接输出
        infoStream.coGroup(priceStream)
                .where(i->i.f0)
                .equalTo(p->p.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(8)))
                .apply(new CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String >() {
                    @Override
                    //该方法的第一个参数为所有数据，第二个参数为相互匹配的数据，最后一个参数为迭代器收集数据
                    public void coGroup(Iterable<Tuple3<String, String, Long>> info, Iterable<Tuple3<String, String, Long>> price, Collector<String> out) throws Exception {
                        String s = RandomStringUtils.randomAlphabetic(8);

                        //收集的两者的所有配备结果（包括没有匹配上的）
                        for (Tuple3<String ,String ,Long > tuple3:info  ){
                            out.collect(s +"===="+tuple3.toString());
                        }
                        //收集两者自建匹配上的数据
                        for (Tuple3<String ,String ,Long>tuple3:price   ){
                            out.collect(s+"----"+tuple3.toString());
                        }
                    }
                }).print("CoGroup").setParallelism(1);

        environment.execute();

    }
}
