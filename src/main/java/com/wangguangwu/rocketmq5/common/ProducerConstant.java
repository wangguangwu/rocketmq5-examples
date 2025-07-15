package common;

/**
 * 生产者组常量类
 *
 * @author wangguangwu
 */
public class ProducerConstant {
    /**
     * 同步生产者组
     */
    public static final String SYNC = "SyncProducer";

    /**
     * 异步生产者组
     */
    public static final String ASYNC = "AsyncProducer";

    /**
     * 单向生产者组
     */
    public static final String ONEWAY = "OnewayProducer";

    /**
     * 顺序消息生产者组
     */
    public static final String ORDER = "OrderProducer";

    /**
     * 延迟消息生产者组
     */
    public static final String SCHEDULE = "ScheduleProducer";

    /**
     * 定时消息生产者组
     */
    public static final String TIME = "TimeProducer";

    /**
     * 批量消息生产者组
     */
    public static final String BATCH = "BatchProducer";

    /**
     * 分批批量消息生产者组
     */
    public static final String SPLIT_BATCH = "SplitBatchProducer";

    /**
     * 事务消息生产者组
     */
    public static final String TRANSACTION = "TransProducer";

    /**
     * 广播消息生产者组
     */
    public static final String BROADCAST = "BroadcastProducer";
}
