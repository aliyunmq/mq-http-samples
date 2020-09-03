const {
  MQClient
} = require('@aliyunmq/mq-http-sdk');

// 设置HTTP接入域名（此处以公共云生产环境为例）
const endpoint = "${HTTP_ENDPOINT}";
// AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
const accessKeyId = "${ACCESS_KEY}";
// SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
const accessKeySecret = "${SECRET_KEY}";

var client = new MQClient(endpoint, accessKeyId, accessKeySecret);

// 所属的 Topic
const topic = "${TOPIC}";
// 您在控制台创建的 Consumer ID(Group ID)
const groupId = "${GROUP_ID}";
// Topic所属实例ID，默认实例为空
const instanceId = "${INSTANCE_ID}";

const consumer = client.getConsumer(instanceId, topic, groupId);

(async function(){
  // 循环消费消息
  while(true) {
    try {
      // 长轮询顺序消费消息, 拿到的消息可能是多个分区的（对于分区顺序）一个分区的内的消息一定是顺序的
      // 对于顺序消费，如果一个分区内的消息只要有没有被确认消费成功的，则对于这个分区下次还会消费到相同的消息
      // 对于一个分区，只有所有消息确认消费成功才能消费下一批消息
      // 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
      res = await consumer.consumeMessageOrderly(
          3, // 一次最多消费3条(最多可设置为16条)
          3 // 长轮询时间3秒（最多可设置为30秒）
          );

      if (res.code == 200) {
        // 消费消息，处理业务逻辑
        console.log("Consume Messages, requestId:%s", res.requestId);
        const handles = res.body.map((message) => {
          console.log("\tMessageId:%s,Tag:%s,PublishTime:%d,NextConsumeTime:%d,FirstConsumeTime:%d,ConsumedTimes:%d,Body:%s" + 
            ",Props:%j,ShardingKey:%s,Prop-A:%s,Tag:%s",
              message.MessageId, message.MessageTag, message.PublishTime, message.NextConsumeTime, message.FirstConsumeTime, message.ConsumedTimes,
              message.MessageBody,message.Properties,message.ShardingKey,message.Properties.a,message.MessageTag);
          return message.ReceiptHandle;
        });

        // message.NextConsumeTime前若不确认消息消费成功，则消息会重复消费
        // 消息句柄有时间戳，同一条消息每次消费拿到的都不一样
        res = await consumer.ackMessage(handles);
        if (res.code != 204) {
          // 某些消息的句柄可能超时了会导致确认不成功
          console.log("Ack Message Fail:");
          const failHandles = res.body.map((error)=>{
            console.log("\tErrorHandle:%s, Code:%s, Reason:%s\n", error.ReceiptHandle, error.ErrorCode, error.ErrorMessage);
            return error.ReceiptHandle;
          });
          handles.forEach((handle)=>{
            if (failHandles.indexOf(handle) < 0) {
              console.log("\tSucHandle:%s\n", handle);
            }
          });
        } else {
          // 消息确认消费成功
          console.log("Ack Message suc, RequestId:%s\n\t", res.requestId, handles.join(','));
        }
      }
    } catch(e) {
      if (e.Code.indexOf("MessageNotExist") > -1) {
        // 没有消息，则继续长轮询服务器
        console.log("Consume Message: no new message, RequestId:%s, Code:%s", e.RequestId, e.Code);
      } else {
        console.log(e);
      }
    }
  }
})();
