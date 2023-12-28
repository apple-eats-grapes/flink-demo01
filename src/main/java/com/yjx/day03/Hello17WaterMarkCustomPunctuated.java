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

import java.util.Locale;/*
*//*
* 根据指定字符生成水位线
* *//*
public class Hello17WaterMarkCustomPunctuated {
    public static void main(String[] args) throws Exception {

        //生产Kafka有序数据数据--模拟弹幕[用户名:消息:时间戳]
        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 1000; i < 2000; i++) {
                if (Math.random() <= 0.99) {
                    KafkaUtil.sendMsg("yjxxt", uname + ":" + i + ":" + System.currentTimeMillis());
                } else {
                    KafkaUtil.sendMsg("yjxxt", uname + ":GameOver:" + System.currentTimeMillis());
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        //读取数据源
        DataStreamSource<String> source = environment.fromSource(KafkaUtil.getKafkaSource("yjxxt", "liyidd"), WatermarkStrategy.noWatermarks(), "Kafka Source");
        //转换数据
        source.map(line -> {
                    return Tuple3.of(line.split(":")[0], line.split(":")[1], Long.parseLong(line.split(":")[2]));
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(new YjxxtPunctuatedWatermarkStrategy())//自定义水位线生成器
                .keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .apply(new WindowFunction<Tuple3<String, String, Long>, String, String, TimeWindow>() {//自定义数据合并。
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, String, Long>> input, Collector<String> out) throws Exception {
                        StringBuffer buffer = new StringBuffer();
                        buffer.append("[key]");
                        for (Tuple3<String, String, Long> tuple3 : input) {
                            buffer.append("[" + tuple3.f1 + "_" + tuple3.f2 + "]");
                        }
                        buffer.append("[" + window + "]");
                        //返回结果
                        out.collect(buffer.toString());
                    }
                }).print();
        //运行环境
        environment.execute();
    }
}

*//**
 * 自定义水位线生成策略
 *//*
class YjxxtPunctuatedWatermarkStrategy implements WatermarkStrategy<Tuple3<String, String, Long>> {

    @Override
    public WatermarkGenerator<Tuple3<String, String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        //定义一个生成器
        return new YjxxtPunctuatedWatermarkGenerator();
    }

    *//**
     * 自定义定点水位线
     *//*
    private class YjxxtPunctuatedWatermarkGenerator implements WatermarkGenerator<Tuple3<String, String, Long>> {

        @Override
        public void onEvent(Tuple3<String, String, Long> tuple3, long l, WatermarkOutput watermarkOutput) {
            //判断是否游戏结束，依据自定字符串生成水位线
            if ("GameOver".equals(tuple3.f1)) {
                watermarkOutput.emitWatermark(new Watermark(tuple3.f2 - 1L));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            //不能执行任何的操作
        }
    }


}

*/