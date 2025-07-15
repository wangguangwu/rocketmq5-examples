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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 异步发送消息示例
 * 特点：发送消息后不等待broker响应，通过回调接收结果，吞吐量高
 * 应用场景：对响应时间敏感的业务场景，如日志收集等
 *
 * @author wangguangwu
 */
public class AsyncProducer {
    
    private static final Logger logger = LoggerFactory.getLogger(AsyncProducer.class);
    
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
        
        // 用于等待异步发送结果
        int messageCount = 10;
        CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        
        // 5. 创建消息并发送
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            // 创建消息对象，指定主题、标签和消息体
            Message message = provider.newMessageBuilder()
                    .setTopic(TopicConstant.SIMPLE)
                    // 设置消息Tag，用于消费端根据指定Tag过滤消息
                    .setTag("TagA")
                    // 设置消息索引键，可根据关键字精确查找某条消息
                    .setKeys("KEY" + index)
                    // 消息体
                    .setBody(("异步消息-" + index).getBytes())
                    .build();
            
            // 异步发送消息，通过CompletableFuture接收发送结果
            CompletableFuture<SendReceipt> future = producer.sendAsync(message);
            future.thenAccept(sendReceipt -> {
                logger.info("{}. 发送异步消息成功，messageId={}", index, sendReceipt.getMessageId());
                countDownLatch.countDown();
            }).exceptionally(throwable -> {
                logger.error("{}. 发送异步消息失败", index, throwable);
                countDownLatch.countDown();
                return null;
            });
        }
        
        // 等待所有异步发送结果返回（最多等待5秒）
        countDownLatch.await(5, TimeUnit.SECONDS);
        
        // 6. 关闭生产者
        producer.close();
    }
}
//