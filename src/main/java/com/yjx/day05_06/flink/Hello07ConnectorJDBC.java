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
public class Hello07ConnectorJDBC {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //获取SourceTable
        tableEnvironment.executeSql("CREATE TABLE flink_jdbc_emp (\n" +
                "  empno INT,\n" +
                "  ename STRING,\n" +
                "  job STRING,\n" +
                "  mgr INT,\n" +
                "  hiredate DATE,\n" +
                "  sal DECIMAL(10, 2),\n" +
                "  comm DECIMAL(10, 2),\n" +
                "  deptno INT,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/scott?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false',\n" +
                "   'table-name' = 'emp',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ");");
        //执行SQL
        tableEnvironment.sqlQuery("select empno,ename,sal from flink_jdbc_emp").execute().print();

    }
}
