package main

import (
	"fmt"
	"github.com/gogap/errors"
	"strings"
	"time"

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
	// 您在控制台创建的 Consumer ID(Group ID)
	groupId := "${GROUP_ID}"

	client := mq_http_sdk.NewAliyunMQClient(endpoint, accessKey, secretKey, "")

	mqConsumer := client.GetConsumer(instanceId, topic, groupId, "")

	for {
		endChan := make(chan int)
		respChan := make(chan mq_http_sdk.ConsumeMessageResponse)
		errChan := make(chan error)
		go func() {
			select {
			case resp := <-respChan:
				{
					// 处理业务逻辑
					var handles []string
					fmt.Printf("Consume %d messages---->\n", len(resp.Messages))
					for _, v := range resp.Messages {
						handles = append(handles, v.ReceiptHandle)
						fmt.Printf("\tMessageID: %s, PublishTime: %d, MessageTag: %s\n"+
							"\tConsumedTimes: %d, FirstConsumeTime: %d, NextConsumeTime: %d\n"+
                            "\tBody: %s\n"+
                            "\tProps: %s\n"+
                            "\tShardingKey: %s\n",
							v.MessageId, v.PublishTime, v.MessageTag, v.ConsumedTimes,
							v.FirstConsumeTime, v.NextConsumeTime, v.MessageBody, v.Properties, v.ShardingKey)
					}

                    // NextConsumeTime前若不确认消息消费成功，则消息会重复消费
                    // 消息句柄有时间戳，同一条消息每次消费拿到的都不一样
                    ackerr := mqConsumer.AckMessage(handles)
					if ackerr != nil {
						// 某些消息的句柄可能超时了会导致确认不成功
						fmt.Println(ackerr)
						for _, errAckItem := range ackerr.(errors.ErrCode).Context()["Detail"].([]mq_http_sdk.ErrAckItem) {
							fmt.Printf("\tErrorHandle:%s, ErrorCode:%s, ErrorMsg:%s\n",
								errAckItem.ErrorHandle, errAckItem.ErrorCode, errAckItem.ErrorMsg)
						}
						time.Sleep(time.Duration(3) * time.Second)
					} else {
						fmt.Printf("Ack ---->\n\t%s\n", handles)
					}

					endChan <- 1
				}
			case err := <-errChan:
				{
					// 没有消息
					if strings.Contains(err.(errors.ErrCode).Error(), "MessageNotExist") {
						fmt.Println("\nNo new message, continue!")
					} else {
						fmt.Println(err)
						time.Sleep(time.Duration(3) * time.Second)
					}
					endChan <- 1
				}
			case <-time.After(35 * time.Second):
				{
					fmt.Println("Timeout of consumer message ??")
					endChan <- 1
				}
			}
		}()

        // 长轮询顺序消费消息, 拿到的消息可能是多个分区的（对于分区顺序）一个分区的内的消息一定是顺序的
        // 对于顺序消费，如果一个分区内的消息只要有没有被确认消费成功的，则对于这个分区下次还会消费到相同的消息
        // 对于一个分区，只有所有消息确认消费成功才能消费下一批消息
        // 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
        mqConsumer.ConsumeMessageOrderly(respChan, errChan,
			3, // 一次最多消费3条(最多可设置为16条)
			3, // 长轮询时间3秒（最多可设置为30秒）
		)
		<-endChan
	}
}
