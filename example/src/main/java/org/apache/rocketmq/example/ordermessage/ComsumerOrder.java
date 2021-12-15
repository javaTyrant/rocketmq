package org.apache.rocketmq.example.ordermessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author lufengxiang
 * @since 2021/12/15
 **/
public class ComsumerOrder {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_3");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicTestjjj", "TagA");
        //消费者并行消费
        consumer.setConsumeThreadMin(3);
        consumer.setConsumeThreadMin(6);
        //
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
//                context.setAutoCommit(false);
            for (MessageExt msg : msgs) {
                System.out.println("queueId:" + msg.getQueueId() + ",orderId:" + new String(msg.getBody()) + ",i:" + msg.getKeys());
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
