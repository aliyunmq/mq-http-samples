//#include <iostream>
#include <fstream>
#include <time.h>
#include "mq_http_sdk/mq_client.h"

using namespace std;
using namespace mq::http::sdk;


int main() {

    MQClient mqClient(
            // 设置HTTP接入域名（此处以公共云生产环境为例）
            "${HTTP_ENDPOINT}",
            // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
            "${ACCESS_KEY}",
            // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
            "${SECRET_KEY}"
            );

    // 所属的 Topic
    string topic = "${TOPIC}";
    // Topic所属实例ID，默认实例为空
    string instanceId = "${INSTANCE_ID}";

    MQProducerPtr producer;
    if (instanceId == "") {
        producer = mqClient.getProducerRef(topic);
    } else {
        producer = mqClient.getProducerRef(instanceId, topic);
    }

    try {
        for (int i = 0; i < 4; i++)
        {
            PublishMessageResponse pmResp;
            if (i % 4 == 0) {
                // publish message, only have body. 
                producer->publishMessage("Hello, mq!", pmResp);
            } else if (i % 4 == 1) {
                // publish message, only have body and tag.
                producer->publishMessage("Hello, mq!have tag!", "tag", pmResp);
            } else if (i % 4 == 2) {
                // publish message, have body,tag,properties and key.
                TopicMessage pubMsg("Hello, mq!have key!");
                pubMsg.putProperty("a",std::to_string(i));
                pubMsg.setMessageKey("MessageKey" + std::to_string(i));
                producer->publishMessage(pubMsg, pmResp);
            } else {
                // publish timer message, message will be consumed after StartDeliverTime
                TopicMessage pubMsg("Hello, mq!timer msg!", "tag");
                // StartDeliverTime is an absolute time in millisecond.
                pubMsg.setStartDeliverTime(time(NULL) * 1000 + 10 * 1000);
                pubMsg.putProperty("b",std::to_string(i));
                pubMsg.putProperty("c",std::to_string(i));
                producer->publishMessage(pubMsg, pmResp);
            }
            cout << "Publish mq message success. Topic is: " << topic 
                << ", msgId is:" << pmResp.getMessageId() 
                << ", bodyMD5 is:" << pmResp.getMessageBodyMD5() << endl;
        }
    } catch (MQServerException& me) {
        cout << "Request Failed: " + me.GetErrorCode() << ", requestId is:" << me.GetRequestId() << endl;
        return -1;
    } catch (MQExceptionBase& mb) {
        cout << "Request Failed: " + mb.ToString() << endl;
        return -2;
    }

    return 0;
}
