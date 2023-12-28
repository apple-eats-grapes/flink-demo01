package com.yjx.SQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello05ConnectorJDBC {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //连接ysql数据库，创建flink表
        tableEnvironment.executeSql("CREATE TABLE flink_jdbc_emp (\n" +
                "  empno INT,\n" +
                "  ename STRING,\n" +
                "  job STRING,\n" +
                "  mgr INT,\n" +
                "  hiredate DATE,\n" +
                "  sal DECIMAL(10,2),\n" +
                "  comm DECIMAL(10,2),\n" +
                "  deptno INT,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/scott?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC',\n" +
                "   'table-name' = 'emp',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ");").print();//connector连接器：用来指定读取文件类型

        tableEnvironment.executeSql("select * from flink_jdbc_emp").print();

    }

}

