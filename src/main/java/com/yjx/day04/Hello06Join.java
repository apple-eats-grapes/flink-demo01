package com.yjx.day04;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.yjx.util.KafkaUtil;

import java.time.Duration;

public class Hello06Join {
    public static void main(String[] args) throws Exception {
        //创建一个线程生成数据
        new Thread(()->{
            for (int i=100 ;i<200;i++){
                //生成一个商品ID
                String gooId = RandomStringUtils.randomAlphabetic(16).toLowerCase();//随机生成一个16位的到小写字母组成的字符串，然后将其全部转成小写
                //发送gooInfo数据【id：info：ts】
                KafkaUtil.sendMsg("t_goodinfo",gooId+":info"+i+":"+System.currentTimeMillis());
                //发送goodPrice数据【id：price：ts】
                KafkaUtil.sendMsg("t_goodprice",gooId+":"+"price"+i+":"+System.currentTimeMillis());
                //休眠线程
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        //创建环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //获取数据源
        DataStreamSource<String> goodInfoSource = environment.fromSource(KafkaUtil.getKafkaSource("t_goodinfo", "l1"), WatermarkStrategy.noWatermarks(), "Kafka Source Info");
        DataStreamSource<String> goodPriceSource = environment.fromSource(KafkaUtil.getKafkaSource("t_goodprice", "liyi"), WatermarkStrategy.noWatermarks(), "Kafka Source Price");
        //添加水位线
        SingleOutputStreamOperator<Tuple3<String, String, Long>> infoStream = goodInfoSource.map(recprd -> {
                    String[] split = recprd.split(":");
                    return Tuple3.of(split[0], split[1], Long.parseLong(split[2]));
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))//设置水位线
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
        //开始进行流Join
        /*infoStream.join(priceStream)//这里join的连接条件，其实是数据中随机生成的随机数
                .where(i->i.f0)//infoStream的join条件
                .equalTo(p->p.f0)//priceStream的join条件
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))//10秒的滚动窗口
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                     @Override
                     public String join(Tuple3<String, String, Long> info, Tuple3<String, String, Long> price) throws Exception {
                         return "[" + info + "][" + price + "]";
                     }
                 }).print("TumblingEventTimeWindows--");*/


        //使用滑动窗口进行join
        infoStream.join(priceStream)
                .where(i->i.f0)
                .equalTo(p->p.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String >() {
                    @Override
                    public String  join(Tuple3<String, String, Long> info, Tuple3<String, String, Long> price) throws Exception {
                        return "["+info+"]["+price+"]";
                    }
                }).print("SlidingEventTimeWindows--");




        //运行环境
        environment.execute();





    }
}
