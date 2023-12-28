package com.yjx.day03;

import com.yjx.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Locale;

/*
* 自定义水位线
*
* */
public class Hello15WaterMarkCustomPeriodicInOrder {
    public static void main(String[] args) throws Exception{
        //生产Kafka有序数据数据--模拟弹幕[用户名:消息:时间戳]
        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 1000; i < 2000; i++) {
                KafkaUtil.sendMsg("yjxxt", uname + ":" + i + ":" + System.currentTimeMillis());
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);


        DataStreamSource<String> source = environment.fromSource(KafkaUtil.getKafkaSource("yjxxt", "yjx")
                , WatermarkStrategy.noWatermarks()
                , "kafak Source");

        //转换数据
        source.map(line->{
            return Tuple3.of(line.split(":")[0], line.split(":")[1], Long.parseLong(line.split(":")[2]));
        }, Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
                .assignTimestampsAndWatermarks(new YjxxtPeriodicWatermarkStrategy())
                .keyBy(tuple3->tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple3<String, String, Long>, String , String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, String, Long>> input, Collector<String> out) throws Exception {
                        StringBuffer buffer = new StringBuffer();

                        for (Tuple3<String,String,Long> tuple:input){
                            buffer.append("[key:"+tuple.f0+",name:"+tuple.f1+",time:"+tuple.f2+"]");
                        }
                        buffer.append("[window:"+window +"]");
                    }
                }).print();
        environment.execute();

    }

}
/*
* 自定义水位线生成策略
* */
class YjxxtPeriodicWatermarkStrategy implements WatermarkStrategy<Tuple3<String, String, Long>> {


    @Override
    public WatermarkGenerator<Tuple3<String, String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        //定义一个生成器
        return new YjxxtPeriodicWatermarkGenerator();
    }
    /*
    * 自定义水位线生成器【有序】
    * */
    private class YjxxtPeriodicWatermarkGenerator implements WatermarkGenerator<Tuple3<String, String, Long>> {
        //声明一个变量，把它作为当前窗口最大的时间戳TS
        private Long maxTs = Long.MIN_VALUE;


        //声明一个变量，作为延迟的时间
        private final Long DURATION_SECONDS = 3L;

        @Override
        public void onEvent(Tuple3<String, String, Long> tuple3, long eventTimestamp, WatermarkOutput output) {
            this.maxTs = tuple3.f2;//将当前流中的新来的就是最大的时间戳
            System.out.println("当前：["+tuple3+"]["+maxTs+"]");
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            System.out.println("YjxxtPeriodicWatermarkGenerator.onPeriodicEmit["+maxTs+"]");
            //设定水位线
            watermarkOutput.emitWatermark(new Watermark(maxTs - DURATION_SECONDS * 1000 - 1L));

        }
    }
}
