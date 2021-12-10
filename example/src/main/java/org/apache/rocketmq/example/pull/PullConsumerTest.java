package org.apache.rocketmq.example.pull;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author lufengxiang
 * @since 2021/12/9
 **/
public class PullConsumerTest {
    public static void main(String[] args) throws Exception {
        Semaphore semaphore = new Semaphore();
        Thread t = new Thread(new Task(semaphore));
        t.start();
        CountDownLatch cdh = new CountDownLatch(1);
        try {
            //程序运行 120s　后介绍
            cdh.await(120 * 1000, TimeUnit.MILLISECONDS);
        } finally {
            semaphore.running = false;
        }
    }

    /**
     * 消息拉取核心实现逻辑
     */
    static class Task implements Runnable {
        Semaphore s;

        public Task(Semaphore s) {
            this.s = s;
        }

        public void run() {
            try {
                DefaultMQPullConsumer consumer = new
                        DefaultMQPullConsumer("please_rename_unique_group_name_4");
                consumer.setNamesrvAddr("127.0.01:9876");
                consumer.start();
                Map<MessageQueue, Long> offsetTable = new HashMap<>();
                //获取该 Topic 的所有队列:首先根据 MQConsumer 的 fetchSubscribeMessageQueues 的方法获取 Topic 的所有队列信息。
                Set<MessageQueue> msgQueueList = consumer.fetchSubscribeMessageQueues("TOPIC_TEST");
                if (msgQueueList != null && !msgQueueList.isEmpty()) {
                    boolean noFoundFlag = false;
                    while (this.s.running) {
                        if (noFoundFlag) { //　没有找到消息，暂停一下消费
                            Thread.sleep(1000);
                        }
                        for (MessageQueue q : msgQueueList) {
                            //然后遍历所有队列，依次通过 MQConsuemr 的 PULL 方法从 Broker 端拉取消息。
                            PullResult pullResult = consumer.pull(q, "*", decivedPulloffset(offsetTable
                                    , q, consumer), 3000);
                            System.out.println("pullStatus:" + pullResult.getPullStatus());
                            //对拉取的消息进行消费处理。
                            switch (pullResult.getPullStatus()) {
                                case FOUND:
                                    doSomething(pullResult.getMsgFoundList());
                                    break;
                                case NO_MATCHED_MSG:
                                    break;
                                case NO_NEW_MSG:
                                case OFFSET_ILLEGAL:
                                    noFoundFlag = true;
                                    break;
                                default:
                                    continue;
                            }
                            //提交位点.通过调用 MQConsumer 的 updateConsumeOffset 方法更新位点，
                            //但需要注意的是这个方法并不是实时向 Broker 提交，而是客户端会启用以线程，默认每隔 5s 向 Broker 集中上报一次。
                            consumer.updateConsumeOffset(q, pullResult.getNextBeginOffset());
                        }
                        System.out.println("balacne queue is empty: " + consumer.
                                fetchMessageQueuesInBalance("TOPIC_TEST").isEmpty());
                    }
                } else {
                    System.out.println("end,because queue is enmpty");
                }
                //
                consumer.shutdown();
                System.out.println("consumer shutdown");
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 拉取到消息后具体的处理逻辑
     */
    private static void doSomething(List<MessageExt> msgs) {
        System.out.println("本次拉取到的消息条数:" + msgs.size());
    }

    public static long decivedPulloffset(Map<MessageQueue, Long> offsetTable,
                                         MessageQueue queue, DefaultMQPullConsumer consumer) throws Exception {
        long offset = consumer.fetchConsumeOffset(queue, false);
        if (offset < 0) {
            offset = 0;
        }
        System.out.println("offset:" + offset);
        return offset;
    }

    static class Semaphore {
        public volatile boolean running = true;
    }
}
