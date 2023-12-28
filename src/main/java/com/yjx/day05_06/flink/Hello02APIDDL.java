package com.yjx.day05_06.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello02APIDDL {
    public static void main(String[] args) {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //获取数据源
        DataStreamSource<String> deptSource = environment.readTextFile("data/dept.txt");
        //创建表：方案1 流表转换
        Table deptTable = tableEnvironment.fromDataStream(deptSource);
        deptTable.execute().print();
        //创建表：方案2 TableAPI
        // tableEnvironment.createTable();
        // tableEnvironment.createTemporaryView();

        //已经过时了：推荐使用上面的方法
        // tableEnvironment.registerDataStream();

        //创建表：方案3 SQL
        // tableEnvironment.executeSql("create table ...");

    }
}
