package simple.consumer;

import common.ConsumerConstant;
import common.MQConstant;
import common.TopicConstant;
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
 * 指定队列的轻量级拉取消费者示例
 * 特点：手动指定要消费的队列，并可以设置从哪个位置开始消费
 * 应用场景：需要精确控制消费队列和起始位置的场景
 *
 * 注意：RocketMQ 5 的 SimpleConsumer 不再支持手动分配队列的方式，
 * 而是通过 ConsumerGroup 和 Topic 的订阅关系自动分配队列
 *
 * @author wangguangwu
 */
public class PullLiteConsumerAssign {
    private static final Logger LOGGER = LoggerFactory.getLogger(PullLiteConsumerAssign.class);
    
    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        // 1. 获取客户端服务提供者
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        
        // 2. 创建客户端配置，使用endpoint而不是namesrv
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(MQConstant.ENDPOINT)
            .build();
        
        // 3. 创建消费者过滤表达式，订阅所有消息
        FilterExpression filterExpression = new FilterExpression("*");
        
        // 4. 创建简单消费者
        // 注意：RocketMQ 5 的 SimpleConsumer 不再支持手动分配队列的方式
        SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup(ConsumerConstant.SIMPLE_PULL)
            .setAwaitDuration(Duration.ofSeconds(5))
            .setSubscriptionExpressions(Collections.singletonMap(TopicConstant.SIMPLE, filterExpression))
            .build();
        
        try {
            // 5. 循环拉取消息
            while (true) {
                // 拉取消息，一次最多拉取10条，超时时间为5秒
                List<MessageView> messageViews = simpleConsumer.receive(10, Duration.ofSeconds(5));
                
                if (messageViews.isEmpty()) {
                    LOGGER.info("没有新消息，等待下一次拉取");
                    Thread.sleep(1000);
                    continue;
                }
                
                // 处理拉取到的消息
                for (MessageView messageView : messageViews) {
                    try {
                        // 处理消息
                        LOGGER.info("接收到消息：{}", new String(messageView.getBody()));
                        
                        // 确认消息消费成功
                        simpleConsumer.ack(messageView);
                    } catch (Exception e) {
                        // 处理消息失败
                        LOGGER.error("处理消息失败", e);
                    }
                }
            }
        } finally {
            // 6. 关闭消费者
            simpleConsumer.close();
        }
    }
}
