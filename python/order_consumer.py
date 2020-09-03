#!/usr/bin/env python
# coding=utf8

from mq_http_sdk.mq_exception import MQExceptionBase
from mq_http_sdk.mq_consumer import *
from mq_http_sdk.mq_client import *

# 初始化 client
mq_client = MQClient(
    # 设置HTTP接入域名（此处以公共云生产环境为例）
    "${HTTP_ENDPOINT}",
    # AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
    "${ACCESS_KEY}",
    # SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
    "${SECRET_KEY}"
)
# 所属的 Topic
topic_name = "${TOPIC}"
# 您在控制台创建的 Consumer ID(Group ID)
group_id = "${GROUP_ID}"
# Topic所属实例ID，默认实例为空None
instance_id = "${INSTANCE_ID}"

consumer = mq_client.get_consumer(instance_id, topic_name, group_id)

# 长轮询顺序消费消息, 拿到的消息可能是多个分区的（对于分区顺序）一个分区的内的消息一定是顺序的
# 对于顺序消费，如果一个分区内的消息只要有没有被确认消费成功的，则对于这个分区下次还会消费到相同的消息
# 对于一个分区，只有所有消息确认消费成功才能消费下一批消息
# 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
wait_seconds = 3
# 一次最多消费3条(最多可设置为16条)
batch = 3
print(("%sConsume And Ak Message From Topic%s\nTopicName:%s\nMQConsumer:%s\nWaitSeconds:%s\n" \
        % (10 * "=", 10 * "=", topic_name, group_id, wait_seconds)))
while True:
    try:
        recv_msgs = consumer.consume_message_orderly(batch, wait_seconds)
        print("=======>Receive %d messages:" % len(recv_msgs))
        for msg in recv_msgs:
            print("\tMessageId: %s, MessageBodyMD5: %s,NextConsumeTime: %s,ConsumedTimes: %s,PublishTime: %s\n\tBody: %s \
                    \n\tReceiptHandle: %s \
                    \n\tProperties: %s,ShardingKey: %s\n" % \
                  (msg.message_id, msg.message_body_md5,
                   msg.next_consume_time, msg.consumed_times,
                   msg.publish_time, msg.message_body,
                   msg.receipt_handle, msg.properties, msg.get_sharding_key()))
    except MQExceptionBase as e:
        if e.type == "MessageNotExist":
            print(("No new message! RequestId: %s" % e.req_id))
            continue

        print(("Consume Message Fail! Exception:%s\n" % e))
        time.sleep(2)
        continue

    # msg.next_consume_time前若不确认消息消费成功，则消息会重复消费
    # 消息句柄有时间戳，同一条消息每次消费拿到的都不一样
    try:
        receipt_handle_list = [msg.receipt_handle for msg in recv_msgs]
        consumer.ack_message(receipt_handle_list)
        print(("========>Ak %s Message Succeed.\n\n" % len(receipt_handle_list)))
    except MQExceptionBase as e:
        print(("\nAk Message Fail! Exception:%s" % e))
        # 某些消息的句柄可能超时了会导致确认不成功
        if e.sub_errors:
            for sub_error in e.sub_errors:
                print(("\tErrorHandle:%s,ErrorCode:%s,ErrorMsg:%s" % \
                      (sub_error["ReceiptHandle"], sub_error["ErrorCode"], sub_error["ErrorMessage"])))
