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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.gsoc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.test.client.consumer.pop.BasePopOrderly;
import org.apache.rocketmq.test.client.rmq.RMQPopClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

public class PopOrderlyReplay extends BasePopOrderly {
    private RMQPopClient baseClient;
    
    @Rule
    public TestWatcher watchman = new TestWatcher() {
        @Override
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }

        @Override
        protected void finished(Description description) {
            System.out.println("Finished test: " + description.getMethodName());
        }
    };

    @Before
    public void setUp() {
        super.setUp();
        baseClient = getRMQPopClient();
    }

    /**
     * 模拟网络问题的 Pop 客户端
     */
    class MockNetworkIssuePopClient extends RMQPopClient {
        private final RMQPopClient delegate;
        private volatile boolean firstRequest = true;
        
        public MockNetworkIssuePopClient(RMQPopClient delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public CompletableFuture<PopResult> popMessageAsync(String brokerAddr, MessageQueue mq,
            long invisibleTime, int maxNums, String consumerGroup, long timeout,
            boolean polling, int initMode, boolean order, String expressionType,
            String expression, String attemptId) {

            if (firstRequest && "attempt-1".equals(attemptId)) {
                firstRequest = false;
                return CompletableFuture.failedFuture(
                    new RemotingTimeoutException("Network disconnected")
                );
            }
            return delegate.popMessageAsync(brokerAddr, mq, invisibleTime, maxNums,
                consumerGroup, timeout, polling, initMode, order,
                expressionType, expression, attemptId);
        }
    }

    /**
     * 测试网络中断导致的 attemptId 丢失场景
     */
    @Test
    public void testPopOrderlyWithNetworkIssue() throws Exception {
        // 1. 发送测试消息
        producer.send(5);

        // 2. 使用模拟网络问题的客户端
        MockNetworkIssuePopClient mockClient = new MockNetworkIssuePopClient(baseClient);

        // 3. 第一次 Pop 请求（会遇到网络问题）
        String attemptId1 = "attempt-1";
        CompletableFuture<Void> future1 = popMessageWithNetworkIssue(mockClient, attemptId1);

        try {
            future1.get(3, TimeUnit.SECONDS);
            fail("Should timeout due to network issue");
        } catch (Exception e) {
            assertThat(e.getCause()).isInstanceOf(RemotingTimeoutException.class);
        }

        // 4. 等待一段时间，模拟网络恢复
        Thread.sleep(2000);

        // 5. 使用新的 attemptId 重试
        String attemptId2 = "attempt-2";
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            popMessageWithNetworkIssue(mockClient, attemptId2).join();
            return msgRecv.size() >= 1;
        });

        // 6. 验证消息接收情况
        assertThat(msgRecv.size()).isGreaterThan(0);
        assertMessageRecvOrder();
    }

    /**
     * 测试消息处理延迟和重试行为
     */
    @Test
    public void testPopOrderlyWithNetworkRecovery() throws Exception {
        // 1. 发送测试消息
        producer.send(10);

        // 2. 记录开始时间
        long startTime = System.currentTimeMillis();

        // 3. 使用模拟网络问题的客户端
        MockNetworkIssuePopClient mockClient = new MockNetworkIssuePopClient(baseClient);

        // 4. 使用相同的 attemptId 进行两次消费
        String attemptId = "same-attempt-id";
        
        // 5. 第一轮消费（不进行 ack）
        CompletableFuture<PopResult> firstPopFuture = mockClient.popMessageAsync(
            brokerAddr, messageQueue,
            TimeUnit.SECONDS.toMillis(5), // invisibleTime
            1,  // maxNums
            group,
            TimeUnit.SECONDS.toMillis(30), // timeout
            false, // polling
            ConsumeInitMode.MIN,
            true, // order
            null, null,
            attemptId
        );

        // 等待第一次消费完成
        PopResult firstPopResult = firstPopFuture.get(3, TimeUnit.SECONDS);
        assertThat(firstPopResult.getMsgFoundList()).isNotEmpty();
        
        // 记录第一次获取消息的时间和消息序号
        long firstPopTime = System.currentTimeMillis();
        MessageExt firstMsg = firstPopResult.getMsgFoundList().get(0);
        System.out.println("SYSTEMOUT: First pop completed at: " + (firstPopTime - startTime) + "ms, message queue offset: " + firstMsg.getQueueOffset());

        // 6. 等待一段时间后进行第二次消费
        System.out.println("SYSTEMOUT: Start second pop");
        
        // 7. 第二次消费（这次会进行 ack）
        String attemptId2 = "attempt-2";
        List<MessageExt> secondPopMsgs = new ArrayList<>();
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            CompletableFuture<PopResult> future = mockClient.popMessageAsync(
                brokerAddr, messageQueue,
                TimeUnit.SECONDS.toMillis(5),
                1,
                group,
                TimeUnit.SECONDS.toMillis(30),
                false,
                ConsumeInitMode.MIN,
                true,
                null, null,
                attemptId2
            );
            
            PopResult result = future.get(3, TimeUnit.SECONDS);
            if (result.getMsgFoundList() != null && !result.getMsgFoundList().isEmpty()) {
                MessageExt msg = result.getMsgFoundList().get(0);
                System.out.println("SYSTEMOUT: Second pop got message with queue offset: " + msg.getQueueOffset());
                secondPopMsgs.addAll(result.getMsgFoundList());
                for (MessageExt messageExt : result.getMsgFoundList()) {
                    onRecvNewMessage(messageExt);
                    ackMessageAsync(messageExt);
                }
                return true;
            }
            return false;
        });

        // 8. 计算总延迟时间
        long totalDelay = System.currentTimeMillis() - startTime;
        System.out.println("SYSTEMOUT: Total processing time: " + totalDelay + "ms");
        System.out.println("SYSTEMOUT: Time between first pop and successful ack: " + (System.currentTimeMillis() - firstPopTime) + "ms");

        // 9. 验证消息顺序和消费情况
        assertThat(msgRecv.size()).isGreaterThan(0);
        assertMessageRecvOrder();
    }

    private CompletableFuture<Void> popMessageWithNetworkIssue(RMQPopClient client, String attemptId) {
        CompletableFuture<PopResult> future = client.popMessageAsync(
            brokerAddr, messageQueue,
            TimeUnit.SECONDS.toMillis(10), // invisibleTime
            1,  // maxNums
            group,
            TimeUnit.SECONDS.toMillis(30), // timeout
            false, // polling
            ConsumeInitMode.MIN,
            true, // order
            null, null,
            attemptId
        );

        return future.thenAccept(popResult -> {
            if (popResult.getMsgFoundList() != null && !popResult.getMsgFoundList().isEmpty()) {
                for (MessageExt messageExt : popResult.getMsgFoundList()) {
                    onRecvNewMessage(messageExt);
                    // 延迟确认消息
                    new Thread(() -> {
                        try {
                            TimeUnit.MILLISECONDS.sleep(100);
                            ackMessageAsync(messageExt);
                        } catch (InterruptedException ignored) {
                        }
                    }).start();
                }
            }
        });
    }
}
