package com.foxmo.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class OrderedProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");

        producer.setNamesrvAddr("192.168.250.128:9876");

        //若为全局有序，泽需要设置Queue数量为1
        //producer.setDefaultTopicQueueNums(1);

        producer.start();

        for (int i = 2; i < 100; i++) {
            Integer orderId = i;
            byte[] body = ("foxmo" + i).getBytes();
            Message message = new Message("TopicA","TagA",body);

            message.setKeys(orderId.toString());
            //send（）的第三个参数值会传递给选择器的select（）的第三个参数
            //该send（）为同步发送
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                /**
                 * 具体的选择算法在该方法中定义
                 * @param mqs   消息队列集合
                 * @param message   发送的消息
                 * @param arg   orderId
                 * @return
                 */
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message message, Object arg) {
                    //以下是使用消息key作为选择的选择算法
                    String keys = message.getKeys();
                    Integer id = Integer.valueOf(keys);

                    //以下是使用arg作为选择key的选择算法
                    //Integer id = (Integer) arg;

                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, orderId);

            System.out.println(sendResult);

        }
        producer.shutdown();
    }
}
