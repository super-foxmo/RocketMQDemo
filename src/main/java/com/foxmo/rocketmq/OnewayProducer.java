package com.foxmo.rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class OnewayProducer {
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

        for (int i = 0; i < 10; i++) {
            byte[] body = ("foxmo" + i).getBytes();
            Message message = new Message("MyTopic", "MyTag", body);
            //单向发送(只管发送消息，不需要droker的ACK相应，故该方法无返回值)
            producer.sendOneway(message);
        }
        //关闭producer
        producer.shutdown();
        System.out.println("producer shutdown");
    }
}
