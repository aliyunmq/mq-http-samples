package main

import (
	"fmt"
	"github.com/gogap/errors"
	"strconv"
	"strings"
	"time"

	"github.com/aliyunmq/mq-http-go-sdk"
)

var loopCount = 0

func ProcessError(err error) {
	// 如果Commit/Rollback时超过了TransCheckImmunityTime（针对发送事务消息的句柄）或者超过10s（针对consumeHalfMessage的句柄）则会失败
	if err == nil {
		return
	}
	fmt.Println(err)
	for _, errAckItem := range err.(errors.ErrCode).Context()["Detail"].([]mq_http_sdk.ErrAckItem) {
		fmt.Printf("\tErrorHandle:%s, ErrorCode:%s, ErrorMsg:%s\n",
			errAckItem.ErrorHandle, errAckItem.ErrorCode, errAckItem.ErrorMsg)
	}
}

func ConsumeHalfMsg(mqTransProducer *mq_http_sdk.MQTransProducer) {
	for {
		if loopCount >= 10 {
			return
		}
		loopCount++
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
							"\tConsumedTimes: %d, FirstConsumeTime: %d, NextConsumeTime: %d\n\tBody: %s\n"+
							"\tProperties:%s, Key:%s, Timer:%d, Trans:%d\n",
							v.MessageId, v.PublishTime, v.MessageTag, v.ConsumedTimes,
							v.FirstConsumeTime, v.NextConsumeTime, v.MessageBody,
							v.Properties, v.MessageKey, v.StartDeliverTime, v.TransCheckImmunityTime)

						a, _ := strconv.Atoi(v.Properties["a"])
						var comRollErr error
						if a == 1 {
							// 确认提交事务消息
							comRollErr = (*mqTransProducer).Commit(v.ReceiptHandle)
							fmt.Println("Commit---------->")
						} else if a == 2 && v.ConsumedTimes > 1 {
							// 确认提交事务消息
							comRollErr = (*mqTransProducer).Commit(v.ReceiptHandle)
							fmt.Println("Commit---------->")
						} else if a == 3 {
							// 确认回滚事务消息
							comRollErr = (*mqTransProducer).Rollback(v.ReceiptHandle)
							fmt.Println("Rollback---------->")
						} else {
							// 什么都不做，下次再检查
							fmt.Println("Unknown---------->")
						}
						ProcessError(comRollErr)
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
					return
				}
			}
		}()

		// 长轮询检查事务半消息
		// 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
		(*mqTransProducer).ConsumeHalfMessage(respChan, errChan,
			3, // 一次最多消费3条(最多可设置为16条)
			3, // 长轮询时间3秒（最多可设置为30秒）
		)
		<-endChan
	}
}

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

	mqTransProducer := client.GetTransProducer(instanceId, topic, groupId)

	// 客户端需要有一个线程或者进程来消费没有确认的事务消息
	// 示例这里启动一个Goroutines来检查没有确认的事务消息
	go ConsumeHalfMsg(&mqTransProducer)

	// 发送4条事务消息，1条发送完就提交，其余3条通过检查事务半消息处理
	for i := 0; i < 4; i++ {
		msg := mq_http_sdk.PublishMessageRequest{
			MessageBody:"I am transaction msg!",
			Properties: map[string]string{"a":strconv.Itoa(i)},
		}
		// 设置事务第一次回查的时间，为相对时间，单位：秒，范围为10~300s之间
		// 第一次事务回查后如果消息没有commit或者rollback，则之后每隔10s左右会回查一次，总共回查一天
		msg.TransCheckImmunityTime = 10

		resp, pubErr := mqTransProducer.PublishMessage(msg)
		if pubErr != nil {
			fmt.Println(pubErr)
			return
		}
		fmt.Printf("Publish ---->\n\tMessageId:%s, BodyMD5:%s, Handle:%s\n",
			resp.MessageId, resp.MessageBodyMD5, resp.ReceiptHandle)
		if i == 0 {
			// 发送完事务消息后能获取到半消息句柄，可以直接commit/rollback事务消息
			ackErr := mqTransProducer.Commit(resp.ReceiptHandle)
			fmt.Println("Commit---------->")
			ProcessError(ackErr)
		}
	}

	for ; loopCount < 10 ; {
		time.Sleep(time.Duration(1) * time.Second)
	}
}
