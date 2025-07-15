package simple.producer;

import common.MQConstant;
import common.TopicConstant;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 同步发送消息示例
 * 特点：发送消息后等待broker响应，可靠性高
 * 应用场景：重要的通知、短信等可靠性要求高的场景
 *
 * @author wangguangwu
 */
public class SyncProducer {

    private static final Logger logger = LoggerFactory.getLogger(SyncProducer.class);

    public static void main(String[] args) throws ClientException, InterruptedException, IOException {
        // 1. 设置接入点地址，需要设置成Proxy的地址和端口列表
        String endpoint = MQConstant.ENDPOINT;

        // 2. 获取客户端服务提供者
        ClientServiceProvider provider = ClientServiceProvider.loadService();

        // 3. 创建客户端配置
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
        ClientConfiguration configuration = builder.build();

        // 4. 初始化Producer，设置通信配置以及预绑定的Topic
        Producer producer = provider.newProducerBuilder()
                .setTopics(TopicConstant.SIMPLE)
                .setClientConfiguration(configuration)
                .build();

        // 5. 创建消息并发送
        for (int i = 0; i < 10; i++) {
            // 创建消息对象，指定主题、标签和消息体
            Message message = provider.newMessageBuilder()
                    .setTopic(TopicConstant.SIMPLE)
                    // 设置消息Tag，用于消费端根据指定Tag过滤消息
                    .setTag("TagA")
                    // 设置消息索引键，可根据关键字精确查找某条消息
                    .setKeys("KEY" + i)
                    // 消息体
                    .setBody(("同步消息-" + i).getBytes())
                    .build();

            try {
                // 发送消息，需要关注发送结果，并捕获失败等异常
                SendReceipt sendReceipt = producer.send(message);
                logger.info("{}. 发送同步消息成功，messageId={}", i, sendReceipt.getMessageId());
            } catch (ClientException e) {
                logger.error("发送消息失败", e);
            }

            // 休眠一段时间
            TimeUnit.SECONDS.sleep(1);
        }

        // 6. 关闭生产者
        producer.close();
    }
}
