package src.main.java.ProducerSingleton;
import com.aliyun.mq.http.MQClient;
import com.aliyun.mq.http.MQProducer;

public class ProducerSingleton {
    // 获取Topic的生产者
    private static volatile MQProducer PRODUCER;

    private ProducerSingleton() {
    }

    public static MQProducer getOneProducer(MQClient mqClient, String instanceId, String topic) {
        if (null == PRODUCER) {
            synchronized (ProducerSingleton.class) {
                if (null == PRODUCER) {
                    PRODUCER = buildProducer(mqClient, instanceId, topic);
                }
            }
        }
        return PRODUCER;
    }

    private static MQProducer buildProducer(MQClient mqClient, String instanceId, String topic) {
        if (instanceId != null && instanceId != "") {
            return mqClient.getProducer(instanceId, topic);
        }

        return mqClient.getProducer(topic);
    }
}
