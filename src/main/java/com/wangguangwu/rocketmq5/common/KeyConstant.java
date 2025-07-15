package com.wangguangwu.rocketmq5.common;

/**
 * 消息索引键常量类
 *
 * @author wangguangwu
 */
public class KeyConstant {
    /**
     * 简单消息索引键前缀
     */
    public static final String SIMPLE_PREFIX = "SIMPLE_KEY_";
    
    /**
     * 订单消息索引键前缀
     */
    public static final String ORDER_PREFIX = "ORDER_KEY_";
    
    /**
     * 延迟消息索引键前缀
     */
    public static final String SCHEDULE_PREFIX = "SCHEDULE_KEY_";
    
    /**
     * 过滤消息索引键前缀
     */
    public static final String FILTER_PREFIX = "FILTER_KEY_";
    
    /**
     * 事务消息索引键前缀
     */
    public static final String TRANSACTION_PREFIX = "TRANSACTION_KEY_";
    
    /**
     * 广播消息索引键前缀
     */
    public static final String BROADCAST_PREFIX = "BROADCAST_KEY_";
    
    /**
     * 生成简单消息的索引键
     * 
     * @param index 索引
     * @return 索引键
     */
    public static String simpleKey(int index) {
        return SIMPLE_PREFIX + index;
    }
    
    /**
     * 生成订单消息的索引键
     * 
     * @param index 索引
     * @return 索引键
     */
    public static String orderKey(int index) {
        return ORDER_PREFIX + index;
    }
    
    /**
     * 生成延迟消息的索引键
     * 
     * @param index 索引
     * @return 索引键
     */
    public static String scheduleKey(int index) {
        return SCHEDULE_PREFIX + index;
    }
    
    /**
     * 生成过滤消息的索引键
     * 
     * @param index 索引
     * @return 索引键
     */
    public static String filterKey(int index) {
        return FILTER_PREFIX + index;
    }
    
    /**
     * 生成事务消息的索引键
     * 
     * @param index 索引
     * @return 索引键
     */
    public static String transactionKey(int index) {
        return TRANSACTION_PREFIX + index;
    }
    
    /**
     * 生成广播消息的索引键
     * 
     * @param index 索引
     * @return 索引键
     */
    public static String broadcastKey(int index) {
        return BROADCAST_PREFIX + index;
    }
}
