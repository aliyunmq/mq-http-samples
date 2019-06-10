const {
  MQClient,
  MessageProperties
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
// Topic所属实例ID，默认实例为空
const instanceId = "${INSTANCE_ID}";
// 您在控制台创建的 Consumer ID(Group ID)
const groupId = "${GROUP_ID}";

const mqTransProducer = client.getTransProducer(instanceId, topic, groupId);

async function processTransResult(res, msgId) {
	if (!res) {
		return;
	}
	if (res.code != 204) {
      // 如果Commit/Rollback时超过了TransCheckImmunityTime（针对发送事务消息的句柄）或者超过NextConsumeTime（针对consumeHalfMessage的句柄）则会失败
      console.log("Commit/Rollback Message Fail:");
      const failHandles = res.body.map((error) => {
        console.log("\tErrorHandle:%s, Code:%s, Reason:%s\n", error.ReceiptHandle, error.ErrorCode, error.ErrorMessage);
        return error.ReceiptHandle;
      });
    } else {
      console.log("Commit/Rollback Message suc!!! %s", msgId);
    }
}

var halfMessageCount = 0;
var halfMessageConsumeCount = 0;

(async function(){
  try {
    // 循环发送4条事务消息
    for(var i = 0; i < 4; i++) {
      let res;
      msgProps = new MessageProperties();
      // 设置属性
      msgProps.putProperty("a", i);
      // 设置KEY
      msgProps.messageKey("MessageKey");
      //设置事务第一次回查的时间，为相对时间，单位：秒，范围为10~300s之间
      // 第一次事务回查后如果消息没有commit或者rollback，则之后每隔10s左右会回查一次，总共回查一天
      msgProps.transCheckImmunityTime(10);
      res = await mqTransProducer.publishMessage("hello mq.", "", msgProps);
      console.log("Publish message: MessageID:%s,BodyMD5:%s,Handle:%s", res.body.MessageId, res.body.MessageBodyMD5, res.body.ReceiptHandle);
      if (res && i == 0) {
      	// 发送完事务消息后能获取到半消息句柄，可以直接commit/rollback事务消息
      	const msgId = res.body.MessageId;
    	  res = await mqTransProducer.commit(res.body.ReceiptHandle);
    	  console.log("Commit msg when publish, %s", msgId);
    	  // 如果Commit/Rollback时超过了TransCheckImmunityTime则会失败
        processTransResult(res, msgId);
      }
    }
  } catch(e) {
    // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
    console.log(e)
  }
})();

// 这里最好有一个单独线程或者进程来消费没有确认的事务消息
// 仅示例：检查没有确认的事务消息
(async function() {
  // 循环检查事务半消息，类似消费普通消息
  while(halfMessageCount < 3 && halfMessageConsumeCount < 15) {
    try {
    	halfMessageConsumeCount++;
      res = await mqTransProducer.consumeHalfMessage(3, 3);
      if (res.code == 200) {
        // 消费消息，处理业务逻辑
        console.log("Consume Messages, requestId:%s", res.requestId);
        res.body.forEach(async (message) => {
          console.log("\tMessageId:%s,Tag:%s,PublishTime:%d,NextConsumeTime:%d,FirstConsumeTime:%d,ConsumedTimes:%d,Body:%s" + 
            ",Props:%j,MessageKey:%s,Prop-A:%s",
              message.MessageId, message.MessageTag, message.PublishTime, message.NextConsumeTime, message.FirstConsumeTime, message.ConsumedTimes,
              message.MessageBody,message.Properties,message.MessageKey,message.Properties.a);
          
          var propA = message.Properties && message.Properties.a ? parseInt(message.Properties.a) : 0;
  				var opResp;
	  			if (propA == 1 || (propA == 2 && message.ConsumedTimes > 1)) {
		  			opResp = await mqTransProducer.commit(message.ReceiptHandle);
		  			console.log("Commit msg when check half, %s", message.MessageId);
		  			halfMessageCount++;
		  		} else if (propA == 3) {
		  			opResp = await mqTransProducer.rollback(message.ReceiptHandle);
		  			console.log("Rollback msg when check half, %s", message.MessageId);
		  			halfMessageCount++;
		  		}
		  		processTransResult(opResp, message.MessageId);
        });
      }
    } catch(e) {
      if (e.Code && e.Code.indexOf("MessageNotExist") > -1) {
        // 没有消息，则继续长轮询服务器
        console.log("Consume Transaction Half msg: no new message, RequestId:%s, Code:%s", e.RequestId, e.Code);
      } else {
        console.log(e);
      }
    }
  }
})();

