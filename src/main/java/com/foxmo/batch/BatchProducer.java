package com.foxmo.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

public class BatchProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("192.168.250.128:9876");
        //指定要发送的消息的最大大小，默认为4M
        //不过，仅修改该属性时不行的，还需要修改broker加载的配置文件中的 MaxMessageSize 属性
        producer.setMaxMessageSize(4 * 1024 * 1024);

        producer.start();
        //定义要发送的消息集合
        ArrayList<Message> messages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            byte[] body = ("foxmo" + i).getBytes();
            Message message = new Message("foxmoTopicA", "foxmoTag", body);

            messages.add(message);
        }

        //定义一个消息列表分割器，将消息列表分割为多个不超出4M大小的小列表
        MessageListSplitter splitter = new MessageListSplitter(messages);

        while(splitter.hasNext()){
            try{
                List<Message> listItem = splitter.next();
                SendResult sendResult = producer.send(listItem);
                System.out.println(sendResult);
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        producer.shutdown();
    }
}
