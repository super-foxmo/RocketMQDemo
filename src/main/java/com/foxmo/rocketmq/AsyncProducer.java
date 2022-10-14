package com.foxmo.rocketmq;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

public class AsyncProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("192.168.250.128:9876");
        //设置异步发送失败后不进行重试发送
        producer.setRetryTimesWhenSendAsyncFailed(0);
        //设置新创建的Topic的Queue数量为2，默认为4
        producer.setDefaultTopicQueueNums(2);
        //设置发送超时时限为5s，默认为3s
        producer.setSendMsgTimeout(5000);
        //启动producer
        producer.start();

        for (int i = 0; i < 10; i++) {
            byte[] body = ("foxmo" + i).getBytes();
            try{
                Message message = new Message("jokerTopic","jokerTag",body);
                //异步发送。指定回调
                producer.send(message, new SendCallback() {
                    //当producer接收到MQ发送类的ACK后就会触发该回调方法的执行
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(sendResult);
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        //由于采用的是异步发送，所以若这里不sleep，泽消息还未发送就会将producer给关闭，并报错
        TimeUnit.SECONDS.sleep(3);
        //关闭producer
        producer.shutdown();

    }
}
