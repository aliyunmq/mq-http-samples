package src.main.java.ConsumerSingleton;
import com.aliyun.mq.http.MQClient;
import com.aliyun.mq.http.MQConsumer;

public class ConsumerSingleton {
    // 获取消费者
    private static volatile MQConsumer CONSUMER;

    private ConsumerSingleton() {
    }

    public static MQConsumer getOneConsumer(MQClient mqClient, String instanceId, String topicName, String consumer, String messageTag) {
        if (null == CONSUMER) {
            synchronized (ConsumerSingleton.class) {
                if (null == CONSUMER) {
                    CONSUMER = buildConsumer(mqClient, instanceId, topicName, consumer ,messageTag);
                }
            }
        }
        return CONSUMER;
    }

    private static MQConsumer buildConsumer(MQClient mqClient, String instanceId, String topicName, String consumer, String messageTag) {
        if (instanceId != null && instanceId != "") {
            return mqClient.getConsumer(instanceId, topicName, consumer, messageTag);
        }

        return mqClient.getConsumer(topicName, consumer);
    }
}
