/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer.listener;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * A MessageListenerOrderly object is used to receive messages orderly. One queue by one thread
 * 消费端的横向扩容或 Broker 端队列个数的变更都会触发消息消费队列的重新负载，在并发消息时在队列负载的时候一个消费队列有可能被多个消费者同时消息，
 * 但顺序消费时并不会出现这种情况，因为顺序消息不仅仅在消费消息时会锁定消息消费队列，在分配到消息队列时，能从该队列拉取消息还需要在 Broker
 * 端申请该消费队列的锁，即同一个时间只有一个消费者能拉取该队列中的消息，确保顺序消费的语义。
 */
public interface MessageListenerOrderly extends MessageListener {
    /**
     * It is not recommend to throw exception,rather than returning ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
     * if consumption failure
     *
     * @param msgs msgs.size() >= 1<br> DefaultMQPushConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @return The consume status
     */
    ConsumeOrderlyStatus consumeMessage(final List<MessageExt> msgs,
                                        final ConsumeOrderlyContext context);
}
