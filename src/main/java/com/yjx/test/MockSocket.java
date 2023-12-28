package com.yjx.test;


import com.google.gson.Gson;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * socket自动化测试
 */
public class MockSocket {
    static ServerSocket serverSocket = null;
    static PrintWriter pw = null;

    public static void main(String[] args) throws Exception{
        String separator,time,type;
        int KeyLength,ValueScope,port;
        String[] socketMetaData = {String.valueOf(SocketProp.port),"kv",":","noTime","1","10"};
        if (args.length < 1)
            System.out.println("目前可输入参数" +
                    "\n\t端口号\t\t->默认值:'13345'" +
                    "\n\t分割字段\t\t->默认值:':'\\只有类型是kv的时候生效" +
                    "\n\t默认类型\t\t->默认值：'kv'\\可选：'json'\\'input(键盘输入)'" +
                    "\n\t是否显示时间\t->默认值：'noTime'\\可选：'Time'" +
                    "\n\tKEY值字段长度\t->默认值：'1'" +
                    "\n\tVALUE值范围\t->默认值：'10'" +
                    "\n当前参数");
        for (int i = 0; i < args.length; i++) {
            System.out.println(" ==>\t"+args[i]);
            socketMetaData[i] = args[i];
        }
        port = Integer.parseInt(socketMetaData[0]);
        type = socketMetaData[1];
        separator = socketMetaData[2];
        time = socketMetaData[3];
        KeyLength = Integer.parseInt(socketMetaData[4]);
        ValueScope = Integer.parseInt(socketMetaData[5]);
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("\n\t=====>服务启动，等待连接");
            Socket socket = serverSocket.accept();
            System.out.println("连接成功，来自：" + socket.getRemoteSocketAddress());
            pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            int j = 0;

//            while (j < 200) {
            while (true) {
                j++;
                String str = "";
                if (type.equals("input"))
                    str = input();
                else if (type.equals("order")) {
                    str = "hello"+separator+j;
                } else if (type.equals("reduce")) {
                    str = "hello"+separator+"1";
                } else {
                    str = socketStream(KeyLength, ValueScope, time, type, separator);
                }
                pw.println(str);
                pw.flush();
//                // TODO INIRIATE 开启 不均匀分配
                for (int i = 0; i < 3; i++) {
                    pw.println("a:1");
                    pw.flush();
                }
                System.out.println(str);
                try {
//                    if (j%5==0) {
//                        Thread.sleep(500L);
//                    }else{
                        Thread.sleep(500L);
//                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                pw.close();
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static String input() {
        String str;
        Scanner scanner = new Scanner(System.in);
        str = scanner.next();
        return str;
    }

    /**
     * 获取每个socket 的值
     * @param KeyLength key长度，为测试为字母，随机
     * @param ValueScope value 的范围 0 - x
     * @param time  是否有时间 time / noTime
     * @param type  类型 kv / json
     * @param separator 分隔符 type是kv的时候生效
     * @return
     */
    private static String socketStream(int KeyLength, int ValueScope, String time, String type, String separator) {
        // 获取kv
        String key = RandomStringUtils.randomAlphabetic(KeyLength).toLowerCase(Locale.ROOT);
        String value = "1";
        if (ValueScope != -1)
            value = String.valueOf(new Random().nextInt(ValueScope));
        String str = "";
        long createTime;
        // 是否需要时间
        if (time.equalsIgnoreCase("time")){
            createTime = System.currentTimeMillis();
        }else{
            createTime = -1L;
        }
        // 判断类型
        if (type.equalsIgnoreCase("kv")){
            str = kv(key, separator, value, createTime);
        } else if (type.equalsIgnoreCase("json")) {
            str = jsonKv(key, value, createTime);
        }
        return str;
    }

    /**
     * json数据简单处理
     * @param key  key
     * @param value value
     * @param createTime 时间戳 -1表示没有
     * @return
     */
    private static String jsonKv(String key, String value, Long createTime) {
        String str;
        HashMap<String,Object> map = new HashMap<>();
        map.put("key", key);
        map.put("value", value);
        if (createTime != -1)
            map.put("createTime", createTime);
        str = simpleObjToJson(map);
        return str;
    }

    /**
     * kv数据简单处理
     * @param key   key
     * @param separator 分隔符
     * @param value value
     * @param createTime 时间戳，-1表示没有
     * @return
     */
    private static String kv(String key, String separator, String value, Long createTime) {
        String str;
        str = key + separator + value;
        if (createTime != -1)
            str += separator + createTime;
        return str;
    }

    /**
     * object 转 json
     * @param obj object
     * @return  String
     */
    private static String simpleObjToJson(Object obj) {
        if (Objects.isNull(obj)) return "";
        try {
            Gson gson = new Gson();
            return gson.toJson(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
