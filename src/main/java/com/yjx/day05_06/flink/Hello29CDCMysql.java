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
public class Hello29CDCMysql {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //创建表
        tableEnvironment.executeSql("CREATE TABLE flink_cdc_dept (\n" +
                "     deptno INT,\n" +
                "     dname STRING,\n" +
                "     loc STRING,\n" +
                "     PRIMARY KEY(deptno) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = '192.168.88.101',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = '123456',\n" +
                "     'database-name' = 'scott',\n" +
                "     'table-name' = 'dept')");

        //简单查询
        tableEnvironment.sqlQuery("select * from flink_cdc_dept").execute().print();
    }
}
