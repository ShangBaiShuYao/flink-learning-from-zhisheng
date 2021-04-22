package com.shangbaishuyao.demo.UDFSource;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Author: shangbaishuyao
 * Date: 16:06 2021/4/22
 * Desc: Socket
 */
//自定义从端口读取数据的Source
public class MySocketSourceUtil implements SourceFunction<WaterSensor> {
        //定义属性信息,主机&端口号
        private String host;
        private Integer port;
        private Boolean running = true;
        Socket socket = null;
        BufferedReader reader = null;
        public MySocketSourceUtil() {}
        public MySocketSourceUtil(String host, Integer port) {
            this.host = host;
            this.port = port;
        }
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            //创建输入流
            socket = new Socket(host, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            //读取数据
            String line = reader.readLine();
            while (running && line != null) {
                //接收数据并发送至Flink系统
                String[] split = line.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                ctx.collect(waterSensor);
                line = reader.readLine();
            }
        }
        @Override
        public void cancel() {
            running = false;
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
}
