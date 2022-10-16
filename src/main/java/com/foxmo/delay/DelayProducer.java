package com.foxmo.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DelayProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");

        producer.setNamesrvAddr("192.168.250.128:9876");

        producer.start();

        byte[] body = "foxmo".getBytes();
        Message message = new Message("TopicB", "TagB", body);
        //指定消息延迟等级为3，即延迟10秒
        message.setDelayTimeLevel(3);
        //发送消息
        SendResult sendResult = producer.send(message);
        //输出消息被发送的时间
        System.out.println("消息被发送的时间:" + new SimpleDateFormat("mm:ss").format(new Date()));

        System.out.println(sendResult);
        //关闭生产者
        producer.shutdown();


    }
}
