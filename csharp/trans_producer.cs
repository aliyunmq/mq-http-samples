using System;
using System.Collections.Generic;
using System.Threading;
using Aliyun.MQ.Model;
using Aliyun.MQ.Model.Exp;
using Aliyun.MQ.Util;

namespace Aliyun.MQ.Sample
{
    public class TransProducerSample
    {
        // 设置HTTP接入域名（此处以公共云生产环境为例）
        private const string _endpoint = "${HTTP_ENDPOINT}";
        // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
        private const string _accessKeyId = "${ACCESS_KEY}";
        // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
        private const string _secretAccessKey = "${SECRET_KEY}";
        // 所属的 Topic
        private const string _topicName = "${TOPIC}";
        // Topic所属实例ID，默认实例为空
        private const string _instanceId = "${INSTANCE_ID}";
        // 您在控制台创建的 Consumer ID(Group ID)
        private const string _groupId = "${GROUP_ID}";

        private static readonly MQClient _client = new Aliyun.MQ.MQClient(_accessKeyId, _secretAccessKey, _endpoint);

        private static readonly MQTransProducer transProducer = _client.GetTransProdcuer(_instanceId, _topicName, _groupId);

        static void ProcessAckError(Exception exception)
        {
            // 如果Commit/Rollback时超过了TransCheckImmunityTime（针对发送事务消息的句柄）或者超过10s（针对consumeHalfMessage的句柄）则会失败
            if (exception is AckMessageException)
            {
                AckMessageException ackExp = (AckMessageException)exception;
                Console.WriteLine("Ack message fail, RequestId:" + ackExp.RequestId);
                foreach (AckMessageErrorItem errorItem in ackExp.ErrorItems)
                {
                    Console.WriteLine("\tErrorHandle:" + errorItem.ReceiptHandle + ",ErrorCode:" + errorItem.ErrorCode + ",ErrorMsg:" + errorItem.ErrorMessage);
                }
            }
        }

        static void ConsumeHalfMessage()
        {
            int count = 0;
            while (true)
            {
                if (count == 3)
                    break;
                try
                {
                    // 检查事务半消息，类似消费普通消息
                    List<Message> messages = null;
                    try
                    {
                        messages = transProducer.ConsumeHalfMessage(3, 3);
                    } catch (Exception exp1) {
                        if (exp1 is MessageNotExistException)
                        {
                            Console.WriteLine(Thread.CurrentThread.Name + " No half message, " + ((MessageNotExistException)exp1).RequestId);
                            continue;
                        }
                        Console.WriteLine(exp1);
                        Thread.Sleep(2000);
                    }

                    if (messages == null)
                        continue;
                    // 处理业务逻辑
                    foreach (Message message in messages)
                    {
                        Console.WriteLine(message);
                        int a = int.Parse(message.GetProperty("a"));
                        uint consumeTimes = message.ConsumedTimes;
                        try {
                            if (a == 1) {
                                // 确认提交事务消息
                                transProducer.Commit(message.ReceiptHandle);
                                count++;
                                Console.WriteLine("Id:" + message.Id + ", commit");
                            } else if (a == 2 && consumeTimes > 1) {
                                // 确认提交事务消息
                                transProducer.Commit(message.ReceiptHandle);
                                count++;
                                Console.WriteLine("Id:" + message.Id + ", commit");
                            } else if (a == 3) {
                                // 确认回滚事务消息
                                transProducer.Rollback(message.ReceiptHandle);
                                count++;
                                Console.WriteLine("Id:" + message.Id + ", rollback");
                            } else {
                                // 什么都不做，下次再检查
                                Console.WriteLine("Id:" + message.Id + ", unkonwn");
                            }
                        } catch (Exception ackError) {
                            ProcessAckError(ackError);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    Thread.Sleep(2000);
                }
            }
        }

        static void Main(string[] args)
        {
            // 客户端需要有一个线程或者进程来消费没有确认的事务消息
            // 示例这里启动一个线程来检查没有确认的事务消息
            Thread consumeHalfThread = new Thread(ConsumeHalfMessage);
            consumeHalfThread.Start();

            try
            {
                // 循环发送4条事务消息, 第一条直接在发送完提交事务, 其它三条根据条件处理
                for (int i = 0; i < 4; i++)
                {
                    TopicMessage sendMsg = new TopicMessage("trans_msg");
                    sendMsg.MessageTag = "a";
                    sendMsg.MessageKey = "MessageKey";
                    sendMsg.PutProperty("a", i.ToString());
                    // 设置事务第一次回查的时间表征该条消息为事务消息，为相对时间，单位：秒，范围为10~300s之间
                    // 第一次事务回查后如果消息没有commit或者rollback，则之后每隔10s左右会回查一次，总共回查一天
                    sendMsg.TransCheckImmunityTime = 10;

                    TopicMessage result = transProducer.PublishMessage(sendMsg);
                    Console.WriteLine("publis message success:" + result);
                    try {
                        if (!string.IsNullOrEmpty(result.ReceiptHandle) && i == 0)
                        {
                            // 发送完事务消息后能获取到半消息句柄，可以直接commit/rollback事务消息
                            transProducer.Commit(result.ReceiptHandle);
                            Console.WriteLine("Id:" + result.Id + ", commit");
                        }
                    } catch (Exception ackError) {
                        ProcessAckError(ackError);
                    }
                }
            } catch (Exception ex) {
                Console.Write(ex);
            }

            consumeHalfThread.Join();
        }
    }
}