package com.yjx.day04.flink;

import com.yjx.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
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
public class Hello08IntervalJoin {
    public static void main(String[] args) throws Exception {

        //创建2个线程生成数据
        new Thread(() -> {
            for (int i = 100; i < 400; i++) {
                //发送goodInfo数据 [id:info:ts]
                KafkaUtil.sendMsg("t_goodinfo", i + ":info" + i + ":" + System.currentTimeMillis());
                //让线程休眠一下
                try {
                    Thread.sleep((int) (Math.random() * 3000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(() -> {
            for (int i = 100; i < 400; i++) {
                //创建goodPrice数据[id:price:ts]
                KafkaUtil.sendMsg("t_goodprice", i + ":" + i + ":" + System.currentTimeMillis());
                //让线程休眠一下
                try {
                    Thread.sleep((int) (Math.random() * 3000));
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
        infoStream.keyBy(info -> info.f0)//对启动的一个流进行key值分区
                .intervalJoin(priceStream.keyBy(price -> price.f0))//对另一个流进行key值分区，然后将两者intervalJoin。
                .between(Time.seconds(-2), Time.seconds(2))//这里是指infoStream中的每个数据时间的前后两秒所表示的时间区间就是priceStream流中数据key相同情况下，时间匹配的窗口
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    //这里的info为匹配数据，price为被join的数据
                    public void processElement(Tuple3<String, String, Long> info, Tuple3<String, String, Long> price,
                                               ProcessJoinFunction<Tuple3<String, String, Long>,
                                               Tuple3<String, String, Long>, String>.Context context,
                                               Collector<String> collector) throws Exception {
                        collector.collect("[" + info + "][" + price + "][" + context + "]");
                    }
                })
                .print("intervalJoin--").setParallelism(2);

        //运行环境
        environment.execute();
    }
}
