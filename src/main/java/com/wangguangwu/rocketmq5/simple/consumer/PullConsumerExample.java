package com.wangguangwu.rocketmq5.simple.consumer;

import com.wangguangwu.rocketmq5.constant.ConsumerConstant;
import com.wangguangwu.rocketmq5.constant.MQConstant;
import com.wangguangwu.rocketmq5.constant.TopicConstant;
import com.wangguangwu.rocketmq5.util.MessageUtil;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * RocketMQ 拉取模式消费者示例
 * 特点：由消费者主动拉取消息，可以自主控制消费速度和消费逻辑
 * 场景：需要自主控制消费进度和消费速度的场景
 *
 * @author wangguangwu
 */
public class PullConsumerExample {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PullConsumerExample.class);
    
    public static void main(String[] args) {
        // 1. 配置 RocketMQ 客户端服务提供者
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        
        // 2. 构建客户端配置（使用代理地址）
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(MQConstant.ENDPOINT)
                .build();
        
        // 3. 订阅表达式（订阅全部消息）
        FilterExpression filterExpression = new FilterExpression("*");
        
        try (SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(ConsumerConstant.SIMPLE_PULL)
                .setAwaitDuration(Duration.ofSeconds(5))
                .setSubscriptionExpressions(Collections.singletonMap(TopicConstant.SIMPLE, filterExpression))
                .build()) {
            
            LOGGER.info("拉取模式消费者已启动，订阅主题: [{}], 消费组: [{}]",
                    TopicConstant.SIMPLE, ConsumerConstant.SIMPLE_PULL);
            
            // 4. 循环拉取消息
            for (int i = 0; i < 10; i++) {
                try {
                    // 拉取消息，一次最多拉取32条，超时时间5秒
                    List<MessageView> messages = simpleConsumer.receive(32, Duration.ofSeconds(5));
                    
                    if (messages.isEmpty()) {
                        LOGGER.info("没有新消息可消费，等待下一次拉取...");
                        Thread.sleep(1000);
                        continue;
                    }
                    
                    LOGGER.info("本次拉取到 {} 条消息", messages.size());
                    
                    // 5. 处理消息
                    for (MessageView message : messages) {
                        try {
                            MessageUtil.processMessage(message);
                            
                            // 确认消息消费成功
                            simpleConsumer.ack(message);
                            LOGGER.info("消息确认成功，MessageId: {}", message.getMessageId());
                        } catch (Exception e) {
                            // 处理消息失败
                            LOGGER.error("处理消息失败: {}", e.getMessage(), e);
                        }
                    }
                } catch (ClientException e) {
                    LOGGER.error("拉取消息失败: {}", e.getMessage(), e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.error("拉取过程被中断: {}", e.getMessage(), e);
                    break;
                }
            }
            
        } catch (ClientException | IOException e) {
            LOGGER.error("初始化 SimpleConsumer 失败: {}", e.getMessage(), e);
        }
        
        LOGGER.info("拉取模式消费者执行完毕，退出程序");
    }
}
