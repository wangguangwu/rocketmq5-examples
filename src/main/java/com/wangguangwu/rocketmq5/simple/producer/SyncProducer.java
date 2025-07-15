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

/**
 * RocketMQ 同步发送消息示例
 * 特点：发送消息后等待 broker 响应，可靠性高
 * 场景：重要通知、账务消息、短信等场景
 *
 * @author wangguangwu
 */
public class SyncProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncProducer.class);

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

            LOGGER.info("同步消息生产者启动成功，目标 Topic：{}", TopicConstant.SIMPLE);

            // 4. 循环构造并发送消息
            for (int i = 0; i < 10; i++) {
                Message message = buildMessage(provider, i);
                try {
                    SendReceipt sendReceipt = producer.send(message);
                    LOGGER.info("{}.消息发送成功，MessageId={}", i, sendReceipt.getMessageId());
                } catch (ClientException e) {
                    LOGGER.error("{}.消息发送失败：{}", i, e.getMessage(), e);
                }
            }

        } catch (ClientException | IOException e) {
            LOGGER.error("初始化 Producer 失败：{}", e.getMessage(), e);
        }

        LOGGER.info("生产者执行完毕，退出程序");
    }

    /**
     * 构建同步消息
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
                .setBody(("同步消息-" + index).getBytes())
                .build();
    }
}