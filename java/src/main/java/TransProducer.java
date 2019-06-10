import com.aliyun.mq.http.MQClient;
import com.aliyun.mq.http.MQTransProducer;
import com.aliyun.mq.http.common.AckMessageException;
import com.aliyun.mq.http.model.Message;
import com.aliyun.mq.http.model.TopicMessage;

import java.util.List;

public class TransProducer {


    static void processCommitRollError(Throwable e) {
        if (e instanceof AckMessageException) {
            AckMessageException errors = (AckMessageException) e;
            System.out.println("Commit/Roll transaction error, requestId is:" + errors.getRequestId() + ", fail handles:");
            if (errors.getErrorMessages() != null) {
                for (String errorHandle :errors.getErrorMessages().keySet()) {
                    System.out.println("Handle:" + errorHandle + ", ErrorCode:" + errors.getErrorMessages().get(errorHandle).getErrorCode()
                            + ", ErrorMsg:" + errors.getErrorMessages().get(errorHandle).getErrorMessage());
                }
            }
        }
    }

    public static void main(String[] args) throws Throwable {
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
        // 您在控制台创建的 Consumer ID(Group ID)
        final String groupId = "${GROUP_ID}";

        final MQTransProducer mqTransProducer = mqClient.getTransProducer(instanceId, topic, groupId);

        for (int i = 0; i < 4; i++) {
            TopicMessage topicMessage = new TopicMessage();
            topicMessage.setMessageBody("trans_msg");
            topicMessage.setMessageTag("a");
            topicMessage.setMessageKey(String.valueOf(System.currentTimeMillis()));
            // 设置事务第一次回查的时间，为相对时间，单位：秒，范围为10~300s之间
            // 第一次事务回查后如果消息没有commit或者rollback，则之后每隔10s左右会回查一次，总共回查一天
            topicMessage.setTransCheckImmunityTime(10);
            topicMessage.getProperties().put("a", String.valueOf(i));

            TopicMessage pubResultMsg = null;
            pubResultMsg = mqTransProducer.publishMessage(topicMessage);
            System.out.println("Send---->msgId is: " + pubResultMsg.getMessageId()
                    + ", bodyMD5 is: " + pubResultMsg.getMessageBodyMD5()
                    + ", Handle: " + pubResultMsg.getReceiptHandle()
            );
            if (pubResultMsg != null && pubResultMsg.getReceiptHandle() != null) {
                if (i == 0) {
                    // 发送完事务消息后能获取到半消息句柄，可以直接commit/rollback事务消息
                    try {
                        mqTransProducer.commit(pubResultMsg.getReceiptHandle());
                        System.out.println(String.format("MessageId:%s, commit", pubResultMsg.getMessageId()));
                    } catch (Throwable e) {
                        // 如果Commit/Rollback时超过了TransCheckImmunityTime则会失败
                        if (e instanceof AckMessageException) {
                            processCommitRollError(e);
                            continue;
                        }
                    }
                }
            }
        }

        // 客户端需要有一个线程或者进程来消费没有确认的事务消息
        // 示例这里启动一个线程来检查没有确认的事务消息
        Thread t = new Thread(new Runnable() {
            public void run() {
                int count = 0;
                while(true) {
                    try {
                        if (count == 3) {
                            break;
                        }
                        List<Message> messages = mqTransProducer.consumeHalfMessage(3, 3);
                        if (messages == null) {
                            System.out.println("No Half message!");
                            continue;
                        }
                        System.out.println(String.format("Half---->MessageId:%s,Properties:%s,Body:%s,Latency:%d",
                                messages.get(0).getMessageId(),
                                messages.get(0).getProperties(),
                                messages.get(0).getMessageBodyString(),
                                System.currentTimeMillis() - messages.get(0).getPublishTime()));

                        for (Message message : messages) {
                            try {
                                if (Integer.valueOf(message.getProperties().get("a")) == 1) {
                                    // 确认提交事务消息
                                    mqTransProducer.commit(message.getReceiptHandle());
                                    count++;
                                    System.out.println(String.format("MessageId:%s, commit", message.getMessageId()));
                                } else if (Integer.valueOf(message.getProperties().get("a")) == 2
                                        && message.getConsumedTimes() > 1) {
                                    // 确认提交事务消息
                                    mqTransProducer.commit(message.getReceiptHandle());
                                    count++;
                                    System.out.println(String.format("MessageId:%s, commit", message.getMessageId()));
                                } else if (Integer.valueOf(message.getProperties().get("a")) == 3) {
                                    // 确认回滚事务消息
                                    mqTransProducer.rollback(message.getReceiptHandle());
                                    count++;
                                    System.out.println(String.format("MessageId:%s, rollback", message.getMessageId()));
                                } else {
                                    // 什么都不做，下次再检查
                                    System.out.println(String.format("MessageId:%s, unknown", message.getMessageId()));
                                }
                            } catch (Throwable e) {
                                // 如果Commit/Rollback时超过了TransCheckImmunityTime（针对发送事务消息的句柄）或者超过10s（针对consumeHalfMessage的句柄）则会失败
                                processCommitRollError(e);
                            }
                        }
                    } catch (Throwable e) {
                        System.out.println(e.getMessage());
                    }
                }
            }
        });

        t.start();

        t.join();

        mqClient.close();
    }

}
