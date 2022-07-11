<?php

require "vendor/autoload.php";

use MQ\Model\TopicMessage;
use MQ\MQClient;

class ProducerTest
{
    private $client;
    private $transProducer;
    private $count;
    private $popMsgCount;

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
        // 您在控制台创建的 Consumer ID(Group ID)
        $groupId = "${GROUP_ID}";
        // Topic所属实例ID，默认实例为空NULL
        $instanceId = "${INSTANCE_ID}";

        $this->transProducer = $this->client->getTransProducer($instanceId,$topic, $groupId);
        $this->count = 0;
        $this->popMsgCount = 0;
    }

    function processAckError($e) {
        if ($e instanceof MQ\Exception\AckMessageException) {
            // 如果Commit/Rollback时超过了TransCheckImmunityTime（针对发送事务消息的句柄）或者超过NextConsumeTime（针对consumeHalfMessage的句柄）则会失败
            printf("Commit/Rollback Error, RequestId:%s\n", $e->getRequestId());
            foreach ($e->getAckMessageErrorItems() as $errorItem) {
                printf("\tReceiptHandle:%s, ErrorCode:%s, ErrorMsg:%s\n", $errorItem->getReceiptHandle(), $errorItem->getErrorCode(), $errorItem->getErrorCode());
            }
        } else {
            print_r($e);
        }
    }

    function consumeHalfMsg() {
        while($this->count < 3 && $this->popMsgCount < 15) {
            $this->popMsgCount++;

            try {
                $messages = $this->transProducer->consumeHalfMessage(4, 3);
            } catch (\Exception $e) {
                if ($e instanceof MQ\Exception\MessageNotExistException) {
                    print "no half transaction message\n";
                    continue;
                }
                print_r($e->getMessage() . "\n");
                sleep(3);
                continue;
            }

            foreach ($messages as $message) {
                printf("ID:%s TAG:%s BODY:%s \nPublishTime:%d, FirstConsumeTime:%d\nConsumedTimes:%d, NextConsumeTime:%d\nPropA:%s\n",
                    $message->getMessageId(), $message->getMessageTag(), $message->getMessageBody(),
                    $message->getPublishTime(), $message->getFirstConsumeTime(), $message->getConsumedTimes(), $message->getNextConsumeTime(),
                    $message->getProperty("a"));
                print_r($message->getProperties());
                $propA = $message->getProperty("a");
                $consumeTimes = $message->getConsumedTimes();
                try {
                    if ($propA == "1") {
                        print "\n commit transaction msg: " . $message->getMessageId() . "\n";
                        $this->transProducer->commit($message->getReceiptHandle());
                        $this->count++;
                    } else if ($propA == "2" && $consumeTimes > 1) {
                        print "\n commit transaction msg: " . $message->getMessageId() . "\n";
                        $this->transProducer->commit($message->getReceiptHandle());
                        $this->count++;
                    } else if ($propA == "3") {
                        print "\n rollback transaction msg: " . $message->getMessageId() . "\n";
                        $this->transProducer->rollback($message->getReceiptHandle());
                        $this->count++;
                    } else {
                        print "\n unknown transaction msg: " . $message->getMessageId() . "\n";
                    }
                } catch (\Exception $e) {
                    $this->processAckError($e);
                }
            }
        }
    }

    public function run()
    {
        for ($i = 0; $i < 4; $i++) {
            $pubMsg = new TopicMessage("xxxxxxxx");
            // 设置属性
            $pubMsg->putProperty("a", $i);
            // 设置消息KEY
            $pubMsg->setMessageKey("MessageKey");
            //设置事务第一次回查的时间，为相对时间，单位：秒，范围为10~300s之间
            // 第一次事务回查后如果消息没有commit或者rollback，则之后每隔10s左右会回查一次，总共回查一天
            $pubMsg->setTransCheckImmunityTime(10);
            $topicMessage = $this->transProducer->publishMessage($pubMsg);

            print "\npublish -> \n\t" . $topicMessage->getMessageId() . " " . $topicMessage->getReceiptHandle() . "\n";

            if ($i == 0) {
                try {
                    // 发送完事务消息后能获取到半消息句柄，可以直接commit/rollback事务消息
                    $this->transProducer->commit($topicMessage->getReceiptHandle());
                    print "\n commit transaction msg when publish: " . $topicMessage->getMessageId() . "\n";
                } catch (\Exception $e) {
                    // 如果Commit/Rollback时超过了TransCheckImmunityTime则会失败
                    $this->processAckError($e);
                }
            }
        }

        // 这里最好有一个单独线程或者进程来消费没有确认的事务消息
        // 仅示例：检查没有确认的事务消息
        $this->consumeHalfMsg();
    }
}


$instance = new ProducerTest();
$instance->run();

?>
