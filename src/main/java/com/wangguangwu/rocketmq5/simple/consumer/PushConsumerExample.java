package com.wangguangwu.rocketmq5.simple.consumer;

import com.wangguangwu.rocketmq5.constant.ConsumerConstant;
import com.wangguangwu.rocketmq5.constant.MQConstant;
import com.wangguangwu.rocketmq5.constant.TopicConstant;
import com.wangguangwu.rocketmq5.util.MessageUtil;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * RocketMQ 推送模式消费者示例
 * 特点：由 Broker 主动推送消息给消费者，实时性高
 * 使用场景：大多数消息消费场景
 *
 * @author wangguangwu
 */
public class PushConsumerExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushConsumerExample.class);

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
                .setMessageListener(MessageUtil::processMessage)
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
}