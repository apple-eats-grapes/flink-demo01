package com.yjx.moke;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Locale;
import java.util.Random;

public class MockSocket {
    static ServerSocket serverSocket = null;
    static PrintWriter pw = null;

    public static void main(String[] args) throws Exception{
        try {
            serverSocket = new ServerSocket(19999);
            System.out.println("服务启动，等待连接");
            Socket socket = serverSocket.accept();
            System.out.println("连接成功，来自：" + socket.getRemoteSocketAddress());
            pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            int j = 0;

            while (j < 50) {
                j++;
                String uname = RandomStringUtils.randomAlphabetic(1).toLowerCase(Locale.ROOT);
               // String str = "hello" + j+" msbjy" + new Random().nextInt(5);
                String str = "{'id':,'createTime'}";
                //String str =  j+" msbjy" + new Random().nextInt(5);
               /*String str = uname+" "+j+" "+System.currentTimeMillis();
               if (j%5==0){
                   str = uname+" "+j+" "+(System.currentTimeMillis()-(long)(new Random().nextInt(5)*1000));
               }*/
                pw.println(str);
                pw.flush();
                System.out.println(str);
                try {
                    if (j%5==0) {
                        Thread.sleep(5000L);
                    }else{
                        Thread.sleep(1000L);
                    }
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
}