package com.yjx.day05_06.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello12FormatJson {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //读取文件
        tableEnvironment.executeSql("CREATE TABLE t_emp (\n" +
                "  empno INT,\n" +
                "  ename STRING,\n" +
                "  job STRING,\n" +
                "  mgr INT,\n" +
                "  hiredate BIGINT,\n" +
                "  sal DECIMAL(10, 2),\n" +
                "  comm DECIMAL(10, 2),\n" +
                "  deptno INT\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',           -- 必选：指定连接器类型\n" +
                "  'path' = 'file:///C:\\Users\\SkyWorth\\IdeaProjects\\flink060106_sql\\data\\emp.txt',  -- 必选：指定路径\n" +
                "  'format' = 'json'                     -- 必选：文件系统连接器指定 format\n" +
                ")");

        tableEnvironment.sqlQuery("select * from t_emp").execute().print();
    }
}
