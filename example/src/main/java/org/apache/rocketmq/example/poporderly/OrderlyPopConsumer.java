package org.apache.rocketmq.example.poporderly;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderlyPopConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderlyPopConsumer.class);
    public static final String TOPIC = "FifoTopic0";
    public static final String CONSUMER_GROUP = "ConsumerGroup0";
    public static void main(String[] args) throws Exception {
        switchPop();
        final AtomicInteger temporarilyBlockedCount = new AtomicInteger(0);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.subscribe(TOPIC, "*");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setPopInvisibleTime(25000);
        consumer.setPullInterval(5000);
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        // 解析消息内容
                        String messageBody = new String(msg.getBody(), StandardCharsets.UTF_8);
                        // 使用正则表达式提取数字
                        String count = messageBody.replaceAll(".*RocketMQ (\\d+).*", "$1");
                        
                        // 1. 模拟业务处理耗时
                        log.info("开始处理消息: [count={}] {}", count, messageBody);
                        Thread.sleep(5000);
                        
                        log.info("消息处理完成 [count={}]", count);
                        // --- 模拟 ACK 丢失 ---
                        int currentCount = temporarilyBlockedCount.incrementAndGet();
                        if (currentCount % 5 == 0) {
                            log.info("此时 ACK 应被 iptables 阻塞");
                            simulateAckLossByBlockingNetwork();
                            Thread.sleep(3000);
                        }
                        // --- 模拟结束 ---

                    } catch (InterruptedException e) {
                        log.warn("消息处理被中断 [msgId={}]", msg.getMsgId());
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    } catch (Exception e) {
                        log.error("消息处理发生未预期异常 [msgId={}]", msg.getMsgId(), e);
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                }
                
                
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.setClientRebalance(false);
        consumer.start();
        log.info("Consumer Started.");
    }
    private static void switchPop() throws Exception {
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setNamesrvAddr("http://localhost:9876");
        
        mqAdminExt.start();
        List<BrokerData> brokerDatas = mqAdminExt.examineTopicRouteInfo(TOPIC).getBrokerDatas();
        //Set<String> brokerAddrs = clusterInfo.getBrokerAddrTable().values().stream().map(BrokerData::selectBrokerAddr).collect(Collectors.toSet());
        for (BrokerData brokerData : brokerDatas) {
            Set<String> brokerAddrs = new HashSet<>(brokerData.getBrokerAddrs().values());
            for (String brokerAddr : brokerAddrs) {
                mqAdminExt.setMessageRequestMode(brokerAddr, TOPIC, CONSUMER_GROUP, MessageRequestMode.POP, 8, 3_000);
            }
        }
    }

    /**
     * Executes the iptables script to temporarily block outgoing ACKs.
     *
     * @return true if the script adding the block rule executed successfully, false otherwise.
     */
    private static boolean simulateAckLossByBlockingNetwork() {
        log.warn("模拟 ACK 丢失：执行阻塞脚本 block_ack_iptables.sh...");
        // !!! Replace with the correct absolute path to your script !!!
        String scriptPath = "/home/vemu6/projects/kc-projects/gsoc/rocketmq/example/src/main/java/org/apache/rocketmq/example/poporderly/block_ack.sh";
        ProcessBuilder processBuilder = new ProcessBuilder("sudo", scriptPath);
        processBuilder.redirectErrorStream(true); // Merge stdout and stderr

        try {
            Process process = processBuilder.start();

            // Capture script output
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            // Wait for the main script process to finish (the part that adds the rule).
            // The script launches the cleanup in the background.
            int exitCode = process.waitFor();

            if (exitCode == 0) {
                log.info("阻塞脚本执行成功 (exit code: {}). ACK 将在接下来几秒内被阻塞.", exitCode);
                log.debug("脚本输出:\n{}", output.toString()); // Log script output at debug level
                return true;
            } else {
                log.error("阻塞脚本执行失败! (exit code: {}).", exitCode);
                log.error("脚本输出:\n{}", output.toString());
                return false;
            }
        } catch (IOException e) {
            log.error("执行阻塞脚本时发生 IO 异常", e);
            return false;
        } catch (InterruptedException e) {
            log.error("等待阻塞脚本执行时被中断", e);
            Thread.currentThread().interrupt(); // Re-interrupt the thread
            return false;
        }
    }
}
