#!/usr/bin/env python
# coding=utf8
import sys

from mq_http_sdk.mq_exception import MQExceptionBase
from mq_http_sdk.mq_producer import *
from mq_http_sdk.mq_client import *
import time
import threading

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
# Topic所属实例ID，默认实例为空None
instance_id = "${INSTANCE_ID}"
# 您在控制台创建的 Consumer ID(Group ID)
group_id = "${GROUP_ID}"

# 循环发布多条事务消息
msg_count = 4
print("%sPublish Transaction Message To %s\nTopicName:%s\nMessageCount:%s\n" \
      % (10 * "=", 10 * "=", topic_name, msg_count))


def process_trans_error(exp):
    print("\nCommit/Roll Transaction Message Fail! Exception:%s" % exp)
    # 如果Commit/Rollback时超过了TransCheckImmunityTime（针对发送事务消息的句柄）
    # 或者超过10s（针对consumeHalfMessage的句柄）则会失败
    if exp.sub_errors:
        for sub_error in exp.sub_errors:
            print("\tErrorHandle:%s,ErrorCode:%s,ErrorMsg:%s" % \
                  (sub_error["ReceiptHandle"], sub_error["ErrorCode"], sub_error["ErrorMessage"]))


# 客户端需要有一个线程或者进程来消费没有确认的事务消息
# 示例这里启动一个线程来检查没有确认的事务消息
class ConsumeHalfMessageThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.count = 0
        # 重新New一个Client
        self.mq_client = MQClient(
            # 设置HTTP接入域名（此处以公共云生产环境为例）
            "${HTTP_ENDPOINT}",
            # AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
            "${ACCESS_KEY}",
            # SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
            "${SECRET_KEY}"
        )
        self.trans_producer = self.mq_client.get_trans_producer(instance_id, topic_name, group_id)

    def run(self):
        while 1:
            if self.count == 3:
                break;
            try:
                half_msgs = self.trans_producer.consume_half_message(1, 3)
                for half_msg in half_msgs:
                    print("Receive Half Message, MessageId: %s\nMessageBodyMD5: %s \
                                                  \nMessageTag: %s\nConsumedTimes: %s \
                                                  \nPublishTime: %s\nBody: %s \
                                                  \nNextConsumeTime: %s \
                                                  \nReceiptHandle: %s \
                                                  \nProperties: %s" % \
                          (half_msg.message_id, half_msg.message_body_md5,
                           half_msg.message_tag, half_msg.consumed_times,
                           half_msg.publish_time, half_msg.message_body,
                           half_msg.next_consume_time, half_msg.receipt_handle, half_msg.properties))

                a = int(half_msg.get_property("a"))
                try:
                    if a == 1:
                        # 确认提交事务消息
                        self.trans_producer.commit(half_msg.receipt_handle)
                        self.count += 1
                        print("------>commit")
                    elif a == 2 and half_msg.consumed_times > 1:
                        # 确认提交事务消息
                        self.trans_producer.commit(half_msg.receipt_handle)
                        self.count += 1
                        print("------>commit")
                    elif a == 3:
                        # 确认回滚事务消息
                        self.trans_producer.rollback(half_msg.receipt_handle)
                        self.count += 1
                        print("------>rollback")
                    else:
                        # 什么都不做，下次再检查
                        print("------>unknown")
                except MQExceptionBase as rec_commit_roll_e:
                    process_trans_error(rec_commit_roll_e)
            except MQExceptionBase as half_e:
                if half_e.type == "MessageNotExist":
                    print("No half message! RequestId: %s" % half_e.req_id)
                    continue

                print("Consume half message Fail! Exception:%s\n" % half_e)
                break


consume_half_thread = ConsumeHalfMessageThread()
consume_half_thread.setDaemon(True)
consume_half_thread.start()

try:
    trans_producer = mq_client.get_trans_producer(instance_id, topic_name, group_id)
    for i in range(msg_count):
        msg = TopicMessage(
            # 消息内容
            "I am test message %s." % i,
            # 消息标签
            ""
        )
        # 设置属性
        msg.put_property("a", i)
        # 设置KEY
        msg.set_message_key("MessageKey")
        # 设置事务第一次回查的时间，为相对时间，单位：秒，范围为10~300s之间
        # 第一次事务回查后如果消息没有commit或者rollback，则之后每隔10s左右会回查一次，总共回查一天
        msg.set_trans_check_immunity_time(10)
        re_msg = trans_producer.publish_message(msg)
        print("Publish Transaction Message Succeed. MessageID:%s, BodyMD5:%s, Handle:%s" \
              % (re_msg.message_id, re_msg.message_body_md5, re_msg.receipt_handle))
        time.sleep(1)
        if i == 0:
            # 发送完事务消息后能获取到半消息句柄，可以直接commit / rollback事务消息
            try:
                trans_producer.commit(re_msg.receipt_handle)
            except MQExceptionBase as pub_commit_roll_e:
                process_trans_error(pub_commit_roll_e)

except MQExceptionBase as pub_e:
    if pub_e.type == "TopicNotExist":
        print("Topic not exist, please create it.")
        sys.exit(1)
    print("Publish Message Fail. Exception:%s" % pub_e)

while 1:
    if not consume_half_thread.isAlive():
        break
    time.sleep(1)
