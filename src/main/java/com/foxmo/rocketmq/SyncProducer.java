package com.foxmo.rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class SyncProducer {
    public static void main(String[] args) throws Exception{
        //创建一个producer（producerGroup名为pg）
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定NameServer地址
        producer.setNamesrvAddr("192.168.250.128:9876");
        //设置当发送失败时重试发送的次数，默认为2次
        producer.setRetryTimesWhenSendFailed(3);
        //设置发送超时时限为5s，默认为3s
        producer.setSendMsgTimeout(5000);
        //开启生产者
        producer.start();

        //生产并发送20条消息
        for (int i = 0; i < 20; i++){
            //消息体
            byte[] body = ("foxmo" + i).getBytes();
            //创建消息对象
            Message message = new Message("foxmoTopic", "foxmoTag", body);
            //同步发送消息
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }

        producer.shutdown();
    }
}
