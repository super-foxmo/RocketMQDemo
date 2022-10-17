package com.foxmo.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class FilterBySQLProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("192.168.250.128:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            try{
                byte[] body = ("foxmo" + i).getBytes();
                Message message = new Message("TopicC", "TagC", body);
                //事先埋入用户属性age
                message.putUserProperty("age",String.valueOf(i));
                SendResult sendResult = producer.send(message);
                System.out.println(sendResult);
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        producer.shutdown();
    }
}
