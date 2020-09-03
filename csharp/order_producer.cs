using System;
using System.Collections.Generic;
using System.Threading;
using Aliyun.MQ.Model;
using Aliyun.MQ.Model.Exp;
using Aliyun.MQ.Util;

namespace Aliyun.MQ.Sample
{
    public class OrderProducerSample
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

        private static MQClient _client = new Aliyun.MQ.MQClient(_accessKeyId, _secretAccessKey, _endpoint);

        static MQProducer producer = _client.GetProducer(_instanceId, _topicName);

        static void Main(string[] args)
        {
            try
            {
                // 循环发送8条消息
                for (int i = 0; i < 8; i++)
                {
                    TopicMessage sendMsg = new TopicMessage("dfadfadfadf", "tag");
                    sendMsg.PutProperty("a", i.ToString());
                    sendMsg.ShardingKey = (i % 2).ToString();
                    TopicMessage result = producer.PublishMessage(sendMsg);
                    Console.WriteLine("publis message success:" + result);
                }
            }
            catch (Exception ex)
            {
                Console.Write(ex);
            }
        }
    }
}
