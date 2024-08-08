package src.main.java.ProducerSingleton;

import com.aliyun.mq.http.MQClient;
import com.aliyun.mq.http.MQTransProducer;

public class TransProducerSingleton {
    // 获取Topic的生产者
    private static volatile MQTransProducer PRODUCER;

    private TransProducerSingleton() {
    }

    public static MQTransProducer getOneProducer(MQClient mqClient, String instanceId, String topic, String groupId) {
        if (null == PRODUCER) {
            synchronized (TransProducerSingleton.class) {
                if (null == PRODUCER) {
                    PRODUCER = buildProducer(mqClient, instanceId, topic, groupId);
                }
            }
        }
        return PRODUCER;
    }

    private static MQTransProducer buildProducer(MQClient mqClient, String instanceId, String topic, String groupId) {
        return mqClient.getTransProducer(instanceId, topic, groupId);
    }
}
