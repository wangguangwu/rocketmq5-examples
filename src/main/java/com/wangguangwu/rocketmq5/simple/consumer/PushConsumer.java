package com.wangguangwu.rocketmq5.simple.consumer;

import com.wangguangwu.rocketmq5.common.ConsumerConstant;
import com.wangguangwu.rocketmq5.common.MQConstant;
import com.wangguangwu.rocketmq5.common.TopicConstant;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * RocketMQ 推送模式消费者示例
 * 特点：由 Broker 主动推送消息给消费者，实时性高
 * 使用场景：大多数消息消费场景
 *
 * @author wangguangwu
 */
public class PushConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushConsumer.class);

    public static void main(String[] args) throws ClientException, InterruptedException {
        // 1. 获取客户端服务提供者
        ClientServiceProvider provider = ClientServiceProvider.loadService();

        // 2. 构建客户端配置（使用 Proxy endpoint）
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(MQConstant.ENDPOINT)
                .build();

        // 3. 订阅表达式（订阅全部消息）
        FilterExpression filterExpression = new FilterExpression("*");

        // 4. 创建并启动消费者
        PushConsumer consumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(ConsumerConstant.SIMPLE_PUSH)
                .setSubscriptionExpressions(
                        Collections.singletonMap(TopicConstant.SIMPLE, filterExpression)
                )
                .setMessageListener(PushConsumer::handleMessage)
                .build();

        LOGGER.info("推送模式消费者已启动，订阅主题: [{}], 消费组: [{}]",
                TopicConstant.SIMPLE, ConsumerConstant.SIMPLE_PUSH);

        // 5. 添加关闭钩子，优雅退出
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOGGER.info("正在关闭消费者...");
                consumer.close();
                LOGGER.info("消费者已关闭");
            } catch (IOException e) {
                LOGGER.error("关闭消费者失败: {}", e.getMessage(), e);
            }
        }));

        // 6. 保持进程运行
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 消息处理逻辑
     *
     * @param message 消息对象
     * @return 消费结果
     */
    private static ConsumeResult handleMessage(MessageView message) {
        try {
            // 读取消息体
            ByteBuffer bodyBuffer = message.getBody();
            byte[] bodyBytes = new byte[bodyBuffer.remaining()];
            bodyBuffer.get(bodyBytes);
            String messageBody = new String(bodyBytes, StandardCharsets.UTF_8);

            // 消息属性
            String messageId = String.valueOf(message.getMessageId());
            String tag = message.getTag().orElse("无标签");
            String keys = String.join(",", message.getKeys());

            LOGGER.info("接收到消息 - ID: {}, Tag: {}, Keys: {}, 内容: {}",
                    messageId, tag, keys, messageBody);

            return ConsumeResult.SUCCESS;
        } catch (Exception e) {
            LOGGER.error("消费消息失败: {}", e.getMessage(), e);
            return ConsumeResult.FAILURE;
        }
    }
}