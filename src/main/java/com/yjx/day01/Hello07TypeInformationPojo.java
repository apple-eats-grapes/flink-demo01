package com.yjx.day01;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Objects;

public class Hello07TypeInformationPojo {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);
        //Transformation+Sink
        source.map(line ->new User((int)(Math.random()*9000+1000)
        ,line.split("-")[0],line.split("-")[1] )).print();//对象的id自动生成，用户名和密码根据用户输入的每一行进行拆分获取，然后打印该对象
        //运行环境
        environment.execute();
    }

}
class User implements Serializable{
    private Integer id;
    private String uname;
    private String passwd;
    public User(){

    }
    public User(Integer id, String uname, String passwd) {
        this.id = id;
        this.uname = uname;
        this.passwd = passwd;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUname() {
        return uname;
    }

    public void setUname(String uname) {
        this.uname = uname;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj )return true;
        if (obj ==null || getClass() !=obj.getClass()) return false;
        User user = (User) obj;
        return Objects.equals(id,user.id) && Objects.equals(uname,user.uname) && Objects.equals(passwd,user.passwd);
    }

    @Override
    public String toString() {
        return "day01.Emp{" +
                "id=" + id +
                ", uname='" + uname + '\'' +
                ", passwd='" + passwd + '\'' +
                '}';
    }
}
