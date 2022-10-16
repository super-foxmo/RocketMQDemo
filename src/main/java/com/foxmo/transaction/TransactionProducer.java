package com.foxmo.transaction;

import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.*;

public class TransactionProducer {
    public static void main(String[] args) throws Exception{
        TransactionMQProducer producer = new TransactionMQProducer("tpg");

        producer.setNamesrvAddr("192.168.250.128:9876");

        /**
         * 定义一个线程池
         * @Param corePoolSize  线程池中核心线程数量
         * @Param maximumPoolSize   线程池中最多线程数
         * @Param keepAliveTime     这是一个时间，当线程池中线程数量大于核心线程数量时，多余空余线程的存活时长
         * @Param unit     时间单位
         * @Param workQueue     临时存放任务的队列，其参数就是队列的长度
         * @Param threadFactory     线程工厂
         */
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");

                return thread;
            }
        });

        //为生产者指定一个线程池
        producer.setExecutorService(executorService);
        //为生产者添加事务监听器
        producer.setTransactionListener(new ICBCTransactionListener());

        producer.start();

        String[] tags = {"TAGA","TAGB","TAGC"};
        for (int i = 0; i < 3 ; i++) {
            byte[] body = ("foxmo" + i).getBytes();
            Message message = new Message("foxmoTopic", tags[i], body);
            //发送事务消息
            //第二个参数用于指定在执行本地事务时要使用的业务参数
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);

            System.out.println("发送结果为：" + sendResult.getSendStatus());
        }
    }
}
