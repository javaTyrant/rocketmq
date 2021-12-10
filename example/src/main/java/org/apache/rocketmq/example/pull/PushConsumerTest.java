package org.apache.rocketmq.example.pull;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragelyByCircle;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * @author lufengxiang
 * @since 2021/12/9
 **/
public class PushConsumerTest {
    public static void main(String[] args) throws InterruptedException, MQClientException {
        //首先 new DefaultMQPushConsumer 对象，并指定一个消费组名。
        DefaultMQPushConsumer consumer = new
                DefaultMQPushConsumer("dw_test_consumer_6");
        //然后设置相关参数，例如 nameSrvAdd、消费失败重试次数、线程数等（可以设置哪些参数将在下篇文章中详细介绍）。
        consumer.setNamesrvAddr("127.0.0.1:9876");
        //通过调用 setConsumeFromWhere 方法指定初次启动时从什么地方消费，默认是最新的消息开始消费。
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TOPIC_TEST", "*");
        //通过调用 setAllocateMessageQueueStrategy 指定队列负载机制，默认平均分配。
        consumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueAveragelyByCircle());
        //通过调用 registerMessageListener 设置消息监听器，即消息处理逻辑，最终返回 CONSUME_SUCCESS（成功消费）或 RECONSUME_LATER（需要重试）。
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            try {
                System.out.printf("%s Receive New Messages: %s %n",
                        Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (Throwable e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
