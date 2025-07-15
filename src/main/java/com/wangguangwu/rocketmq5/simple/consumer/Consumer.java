package simple.consumer;

import common.ConsumerConstant;
import common.MQConstant;
import common.TopicConstant;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * 推送模式消费者示例
 * 特点：由Broker主动推送消息给消费者，实时性高
 * 应用场景：大多数消息消费场景
 *
 * @author wangguangwu
 */
public class Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    
    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        // 1. 获取客户端服务提供者
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        
        // 2. 创建客户端配置，使用endpoint而不是namesrv
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(MQConstant.ENDPOINT)
            .build();
        
        // 3. 创建消费者过滤表达式，订阅所有消息
        FilterExpression filterExpression = new FilterExpression("*");
        
        // 4. 创建消费者
        PushConsumer consumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup(ConsumerConstant.SIMPLE_PUSH)
            .setSubscriptionExpressions(Collections.singletonMap(TopicConstant.SIMPLE, filterExpression))
            .setMessageListener(messageView -> {
                // 处理接收到的消息
                LOGGER.info("接收到消息：{}", new String(messageView.getBody()));
                
                // 返回消费状态：成功
                return ConsumeResult.SUCCESS;
            })
            .build();
        
        // 5. 消费者已启动
        LOGGER.info("消费者已启动");
        
        // 保持进程运行
        Thread.sleep(Long.MAX_VALUE);
    }
}
