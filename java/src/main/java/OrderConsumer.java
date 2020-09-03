import com.aliyun.mq.http.MQClient;
import com.aliyun.mq.http.MQConsumer;
import com.aliyun.mq.http.common.AckMessageException;
import com.aliyun.mq.http.model.Message;

import java.util.ArrayList;
import java.util.List;

public class OrderConsumer {

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
        // 您在控制台创建的 Consumer ID(Group ID)
        final String groupId = "${GROUP_ID}";
        // Topic所属实例ID，默认实例为空
        final String instanceId = "${INSTANCE_ID}";

        final MQConsumer consumer;
        if (instanceId != null && instanceId != "") {
            consumer = mqClient.getConsumer(instanceId, topic, groupId, null);
        } else {
            consumer = mqClient.getConsumer(topic, groupId);
        }

        // 在当前线程循环消费消息，建议是多开个几个线程并发消费消息
        do {
            List<Message> messages = null;

            try {
                // 长轮询顺序消费消息, 拿到的消息可能是多个分区的（对于分区顺序）一个分区的内的消息一定是顺序的
                // 对于顺序消费，如果一个分区内的消息只要有没有被确认消费成功的，则对于这个分区下次还会消费到相同的消息
                // 对于一个分区，只有所有消息确认消费成功才能消费下一批消息
                // 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
                messages = consumer.consumeMessageOrderly(
                        3,// 一次最多消费3条(最多可设置为16条)
                        3// 长轮询时间3秒（最多可设置为30秒）
                );
            } catch (Throwable e) {
                e.printStackTrace();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            // 没有消息
            if (messages == null || messages.isEmpty()) {
                System.out.println(Thread.currentThread().getName() + ": no new message, continue!");
                continue;
            }

            // 处理业务逻辑
            System.out.println("Receive " + messages.size() + " messages:");
            for (Message message : messages) {
                System.out.println(message);
                System.out.println("ShardingKey: " + message.getShardingKey() + ", a:" + message.getProperties().get("a"));
            }

            // Message.nextConsumeTime前若不确认消息消费成功，则消息会重复消费
            // 消息句柄有时间戳，同一条消息每次消费拿到的都不一样
            {
                List<String> handles = new ArrayList<String>();
                for (Message message : messages) {
                    handles.add(message.getReceiptHandle());
                }

                try {
                    consumer.ackMessage(handles);
                } catch (Throwable e) {
                    // 某些消息的句柄可能超时了会导致确认不成功
                    if (e instanceof AckMessageException) {
                        AckMessageException errors = (AckMessageException) e;
                        System.out.println("Ack message fail, requestId is:" + errors.getRequestId() + ", fail handles:");
                        if (errors.getErrorMessages() != null) {
                            for (String errorHandle :errors.getErrorMessages().keySet()) {
                                System.out.println("Handle:" + errorHandle + ", ErrorCode:" + errors.getErrorMessages().get(errorHandle).getErrorCode()
                                        + ", ErrorMsg:" + errors.getErrorMessages().get(errorHandle).getErrorMessage());
                            }
                        }
                        continue;
                    }
                    e.printStackTrace();
                }
            }
        } while (true);
    }
}
