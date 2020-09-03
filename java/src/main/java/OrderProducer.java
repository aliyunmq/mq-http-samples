import com.aliyun.mq.http.MQClient;
import com.aliyun.mq.http.MQProducer;
import com.aliyun.mq.http.model.TopicMessage;

import java.util.Date;

public class OrderProducer {

    public static void main(String[] args) {
        MQClient mqClient = new MQClient(
                // 设置HTTP接入域名（此处以公共云生产环境为例）
                "${HTTP_ENDPOINT}",
                // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
                "${ACCESS_KEY}",
                // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
                "${SECRET_KEY}"
        );

        // 所属的 Topic
        final String topic = "${TOPIC}";
        // Topic所属实例ID，默认实例为空
        final String instanceId = "${INSTANCE_ID}";

        // 获取Topic的生产者
        MQProducer producer;
        if (instanceId != null && instanceId != "") {
            producer = mqClient.getProducer(instanceId, topic);
        } else {
            producer = mqClient.getProducer(topic);
        }

        try {
            // 循环发送8条消息
            for (int i = 0; i < 8; i++) {
                TopicMessage pubMsg = new TopicMessage(
                        // 消息内容
                        "hello mq!".getBytes(),
                        // 消息标签
                        "A"
                );
                // 设置顺序消息的分区KEY
                pubMsg.setShardingKey(String.valueOf(i % 2));
                pubMsg.getProperties().put("a", String.valueOf(i));
                // 同步发送消息，只要不抛异常就是成功
                TopicMessage pubResultMsg = producer.publishMessage(pubMsg);

                // 同步发送消息，只要不抛异常就是成功
                System.out.println(new Date() + " Send mq message success. Topic is:" + topic + ", msgId is: " + pubResultMsg.getMessageId()
                        + ", bodyMD5 is: " + pubResultMsg.getMessageBodyMD5());
            }
        } catch (Throwable e) {
            // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
            System.out.println(new Date() + " Send mq message failed. Topic is:" + topic);
            e.printStackTrace();
        }

        mqClient.close();
    }

}
