using System;
using System.Collections.Generic;
using System.Threading;
using Aliyun.MQ.Model;
using Aliyun.MQ.Model.Exp;
using Aliyun.MQ;

namespace Aliyun.MQ.Sample
{
	public class ConsumerSample
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

        private static MQClient _client = new Aliyun.MQ.MQClient(_accessKeyId, _secretAccessKey, _endpoint);
        static MQConsumer consumer = _client.GetConsumer(_instanceId, _topicName, _groupId, null);

        static void Main(string[] args)
        {
            // 在当前线程循环消费消息，建议是多开个几个线程并发消费消息
            while (true)
            {
                try
                {
                    // 长轮询消费消息
                    // 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
                    List<Message> messages = null;

                    try
                    {
                        messages = consumer.ConsumeMessage(
                            3, // 一次最多消费3条(最多可设置为16条)
                            3 // 长轮询时间3秒（最多可设置为30秒）
                        );
                    }
                    catch (Exception exp1)
                    {
                        if (exp1 is MessageNotExistException)
                        {
                            Console.WriteLine(Thread.CurrentThread.Name + " No new message, " + ((MessageNotExistException)exp1).RequestId);
                            continue;
                        }
                        Console.WriteLine(exp1);
                        Thread.Sleep(2000);
                    }

                    if (messages == null)
                    {
                        continue;
                    }

                    List<string> handlers = new List<string>();
                    Console.WriteLine(Thread.CurrentThread.Name + " Receive Messages:");
                    // 处理业务逻辑
                    foreach (Message message in messages)
                    {
                        Console.WriteLine(message);
                        Console.WriteLine("Property a is:" + message.GetProperty("a"));
                        handlers.Add(message.ReceiptHandle);
                    }
                    // Message.nextConsumeTime前若不确认消息消费成功，则消息会重复消费
                    // 消息句柄有时间戳，同一条消息每次消费拿到的都不一样
                    try
                    {
                        consumer.AckMessage(handlers);
                        Console.WriteLine("Ack message success:");
                        foreach (string handle in handlers)
                        {
                            Console.Write("\t" + handle);
                        }
                        Console.WriteLine();
                    }
                    catch (Exception exp2)
                    {
                        // 某些消息的句柄可能超时了会导致确认不成功
                        if (exp2 is AckMessageException)
                        {
                            AckMessageException ackExp = (AckMessageException)exp2;
                            Console.WriteLine("Ack message fail, RequestId:" + ackExp.RequestId);
                            foreach (AckMessageErrorItem errorItem in ackExp.ErrorItems)
                            {
                                Console.WriteLine("\tErrorHandle:" + errorItem.ReceiptHandle + ",ErrorCode:" + errorItem.ErrorCode + ",ErrorMsg:" + errorItem.ErrorMessage);
                            }
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
    }
}