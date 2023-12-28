package com.yjx.SQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello8ConnectorFileSystem {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //读取文件
        tableEnvironment.executeSql("CREATE TABLE t_dept (\n" +
                "  deptno INT,\n" +
                "  dname STRING,\n" +
                "  loc STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',           -- 必选：指定连接器类型\n" +
                "  'path' = 'file:///E:\\java_not\\flink-demo01\\data\\dept.txt',  -- 必选：指定路径\n" +
                "  'format' = 'csv'                     -- 必选：文件系统连接器指定 format\n" +
                ")");

        tableEnvironment.sqlQuery("select * from t_dept").execute().print();

    }
}
