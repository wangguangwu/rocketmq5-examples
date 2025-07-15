package com.wangguangwu.rocketmq5.constant;

/**
 * 消费者组常量类
 *
 * @author wangguangwu
 */
public class ConsumerConstant {

    /**
     * 简单推送消费者组
     */
    public static final String SIMPLE_PUSH = "SimplePushConsumer";

    /**
     * 简单拉取消费者组
     */
    public static final String SIMPLE_PULL = "SimplePullConsumer";

    /**
     * 顺序消息消费者组
     */
    public static final String ORDER = "OrderConsumer";

    /**
     * 广播消息消费者组
     */
    public static final String BROADCAST = "BroadCastConsumer";

    /**
     * 延迟消息消费者组
     */
    public static final String SCHEDULE = "ScheduleConsumer";

    /**
     * 定时消息消费者组
     */
    public static final String TIME = "TimeConsumer";

    /**
     * 事务消息消费者组
     */
    public static final String TRANSACTION = "TransactionConsumer";
}
