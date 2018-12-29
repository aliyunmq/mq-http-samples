<?php

require "vendor/autoload.php";

use MQ\Model\TopicMessage;
use MQ\MQClient;

class ProducerTest
{
    private $client;
    private $producer;

    public function __construct()
    {
        $this->client = new MQClient(
            // 设置HTTP接入域名（此处以公共云生产环境为例）
            "${HTTP_ENDPOINT}",
            // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
            "${ACCESS_KEY}",
            // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
            "${SECRET_KEY}"
        );

        // 所属的 Topic
        $topic = "${TOPIC}";
        // Topic所属实例ID，默认实例为空NULL
        $instanceId = "${INSTANCE_ID}";

        $this->producer = $this->client->getProducer($instanceId, $topic);
    }

    public function run()
    {
        try
        {
            for ($i=1; $i<=100; $i++)
            {
                $publishMessage = new TopicMessage(
                    "xxxxxxxx"// 消息内容
                );
                $result = $this->producer->publishMessage($publishMessage);

                print "Send mq message success. msgId is:" . $result->getMessageId() . ", bodyMD5 is:" . $result->getMessageBodyMD5() . "\n";
            }
        } catch (\Exception $e) {
            print_r($e->getMessage() . "\n");
        }
    }
}


$instance = new ProducerTest();
$instance->run();

?>
