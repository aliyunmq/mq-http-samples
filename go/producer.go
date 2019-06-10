package main

import (
    "fmt"
    "time"
    "strconv"

    "github.com/aliyunmq/mq-http-go-sdk"
)

func main() {
    // 设置HTTP接入域名（此处以公共云生产环境为例）
    endpoint := "${HTTP_ENDPOINT}"
    // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
    accessKey := "${ACCESS_KEY}"
    // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
    secretKey := "${SECRET_KEY}"
    // 所属的 Topic
    topic := "${TOPIC}"
    // Topic所属实例ID，默认实例为空
    instanceId := "${INSTANCE_ID}"

    client := mq_http_sdk.NewAliyunMQClient(endpoint, accessKey, secretKey, "")

    mqProducer := client.GetProducer(instanceId, topic)
    // 循环发送4条消息
    for i := 0; i < 4; i++ {
        var msg mq_http_sdk.PublishMessageRequest
        if i%2 == 0 {
            msg = mq_http_sdk.PublishMessageRequest{
                MessageBody: "hello mq!",         //消息内容
                MessageTag:  "",                  // 消息标签
                Properties:  map[string]string{}, // 消息属性
            }
            // 设置KEY
            msg.MessageKey = "MessageKey"
            // 设置属性
            msg.Properties["a"] = strconv.Itoa(i)
        } else {
            msg = mq_http_sdk.PublishMessageRequest{
                MessageBody: "hello mq timer!",         //消息内容
                MessageTag:  "",                  // 消息标签
                Properties:  map[string]string{}, // 消息属性
            }
            // 设置属性
            msg.Properties["a"] = strconv.Itoa(i)
            // 定时消息, 定时时间为10s后, 值为毫秒级别的Unix时间戳
            msg.StartDeliverTime = time.Now().UTC().Unix() * 1000 + 10 * 1000
        }
        ret, err := mqProducer.PublishMessage(msg)

        if err != nil {
            fmt.Println(err)
            return
        } else {
            fmt.Printf("Publish ---->\n\tMessageId:%s, BodyMD5:%s, \n", ret.MessageId, ret.MessageBodyMD5)
        }
        time.Sleep(time.Duration(100) * time.Millisecond)
    }
}
