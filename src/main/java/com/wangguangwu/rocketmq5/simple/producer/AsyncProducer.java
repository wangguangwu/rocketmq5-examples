package com.wangguangwu.rocketmq5.simple.producer;

import com.wangguangwu.rocketmq5.constant.KeyConstant;
import com.wangguangwu.rocketmq5.constant.MQConstant;
import com.wangguangwu.rocketmq5.constant.TagConstant;
import com.wangguangwu.rocketmq5.constant.TopicConstant;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * RocketMQ 异步发送消息示例
 * 特点：发送消息后不等待 broker 响应，通过回调接收结果，吞吐量高
 * 场景：日志收集、监控数据等对响应时间敏感的业务场景
 *
 * @author wangguangwu
 */
public class AsyncProducer {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncProducer.class);
    
    public static void main(String[] args) {
        // 1. 配置 RocketMQ 客户端服务提供者
        ClientServiceProvider provider = ClientServiceProvider.loadService();

        // 2. 构建客户端配置（使用代理地址）
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
                .setEndpoints(MQConstant.ENDPOINT)
                .build();

        // 3. 初始化 Producer（设置 Topic）
        try (Producer producer = provider.newProducerBuilder()
                .setTopics(TopicConstant.SIMPLE)
                .setClientConfiguration(configuration)
                .build()) {
            
            LOGGER.info("异步消息生产者启动成功，目标 Topic：{}", TopicConstant.SIMPLE);
            
            // 用于等待异步发送结果
            int messageCount = 10;
            CountDownLatch countDownLatch = new CountDownLatch(messageCount);
            
            // 4. 循环构造并发送消息
            for (int i = 0; i < messageCount; i++) {
                final int index = i;
                Message message = buildMessage(provider, index);
                
                // 异步发送消息，通过 CompletableFuture 接收发送结果
                CompletableFuture<SendReceipt> future = producer.sendAsync(message);
                future.thenAccept(sendReceipt -> {
                    LOGGER.info("{}.异步消息发送成功，MessageId={}", index, sendReceipt.getMessageId());
                    countDownLatch.countDown();
                }).exceptionally(throwable -> {
                    LOGGER.error("{}.异步消息发送失败：{}", index, throwable.getMessage(), throwable);
                    countDownLatch.countDown();
                    return null;
                });
            }
            
            // 5. 等待所有异步发送结果返回（最多等待5秒）
            try {
                boolean completed = countDownLatch.await(5, TimeUnit.SECONDS);
                if (!completed) {
                    LOGGER.warn("部分消息发送可能未完成，超时退出");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error("等待异步结果被中断: {}", e.getMessage(), e);
            }
            
        } catch (ClientException | IOException e) {
            LOGGER.error("初始化 Producer 失败：{}", e.getMessage(), e);
        }
        
        LOGGER.info("生产者执行完毕，退出程序");
    }
    
    /**
     * 构建异步消息
     *
     * @param provider 客户端服务提供者
     * @param index    消息序号
     * @return 构建好的消息对象
     */
    private static Message buildMessage(ClientServiceProvider provider, int index) {
        return provider.newMessageBuilder()
                .setTopic(TopicConstant.SIMPLE)
                .setTag(TagConstant.TAG_A)
                .setKeys(KeyConstant.simpleKey(index))
                .setBody(("异步消息-" + index).getBytes())
                .build();
    }
}
