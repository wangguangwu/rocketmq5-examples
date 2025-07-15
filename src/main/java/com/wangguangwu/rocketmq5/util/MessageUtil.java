package com.wangguangwu.rocketmq5.util;

import com.wangguangwu.rocketmq5.simple.consumer.PushConsumerExample;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author wangguangwu
 */
public final class MessageUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageUtil.class);

    /**
     * 消息处理逻辑
     *
     * @param message 消息对象
     * @return 消费结果
     */
    public static ConsumeResult processMessage(MessageView message) {
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
