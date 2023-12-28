package com.yjx.SQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello01APIEnvironment {

    public static void main(String[] args) {
        //创建环境配置
        EnvironmentSettings environmentSettings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()//创建流处理
//                .inBatchMode()//创建批处理
                .build();
        //这里的TableEnvironment在1.12之前的版本是单独用来批处理任务的，1.12之后就直接流和批直接合并。
        TableEnvironment tableEnvironment01 = TableEnvironment.create(environmentSettings);

        //合并后为
        TableEnvironment environment1 = TableEnvironment.create(EnvironmentSettings.inStreamingMode().newInstance().build());


        //StreamExecutionEnvironment为流处理环境，1.12之后批处理与流处理最终TableAPI和SQL都会转成DataStream程序
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment02 = StreamTableEnvironment.create(environment);

        //合并后为
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());


    }
}
