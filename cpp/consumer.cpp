#include <vector>
#include <fstream>
#include "mq_http_sdk/mq_client.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

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
    // 您在控制台创建的 Consumer ID(Group ID)
    string groupId = "${GROUP_ID}";
    // Topic所属实例ID，默认实例为空
    string instanceId = "${INSTANCE_ID}";

    MQConsumerPtr consumer;
    if (instanceId == "") {
        consumer = mqClient.getConsumerRef(topic, groupId);
    } else {
        consumer = mqClient.getConsumerRef(instanceId, topic, groupId, "");
    }

    do {
        try {
            std::vector<Message> messages;
            // 长轮询消费消息
            // 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
            consumer->consumeMessage(
                    3,//一次最多消费3条(最多可设置为16条)
                    3,//长轮询时间3秒（最多可设置为30秒） 
                    messages
            );
            cout << "Consume: " << messages.size() << " Messages!" << endl;

            // 处理消息
            std::vector<std::string> receiptHandles;
            for (std::vector<Message>::iterator iter = messages.begin();
                    iter != messages.end(); ++iter)
            {
                cout << "MessageId: " << iter->getMessageId()
                    << " PublishTime: " << iter->getPublishTime()
                    << " Tag: " << iter->getMessageTag()
                    << " FirstConsumeTime: " << iter->getFirstConsumeTime()
                    << " NextConsumeTime: " << iter->getNextConsumeTime()
                    << " ConsumedTimes: " << iter->getConsumedTimes() << endl;
                receiptHandles.push_back(iter->getReceiptHandle());
            }

            // 确认消息消费成功
            // 5分钟之内若不确认消息消费成功，则消息会重复消费
            AckMessageResponse bdmResp;
            consumer->ackMessage(receiptHandles, bdmResp);
            if (!bdmResp.isSuccess()) {
                // 某些消息的句柄可能超时了会导致确认不成功
                const std::vector<AckMessageFailedItem>& failedItems =
                    bdmResp.getAckMessageFailedItem();
                for (std::vector<AckMessageFailedItem>::const_iterator iter = failedItems.begin();
                        iter != failedItems.end(); ++iter)
                {
                    cout << "AckFailedItem: " << iter->errorCode
                        << "  " << iter->receiptHandle << endl;
                }
            } else {
                cout << "Ack: " << messages.size() << " messages suc!" << endl;
            }
        } catch (MQServerException& me) {
            if (me.GetErrorCode() == "MessageNotExist") {
                cout << "No message to consume! RequestId: " + me.GetRequestId() << endl;
                continue;
            }
            cout << "Request Failed: " + me.GetErrorCode() + ".RequestId: " + me.GetRequestId() << endl;
#ifdef WIN32
            Sleep(2000);
#else
            usleep(2000 * 1000);
#endif
        } catch (MQExceptionBase& mb) {
            cout << "Request Failed: " + mb.ToString() << endl;
#ifdef WIN32
            Sleep(2000);
#else
            usleep(2000 * 1000);
#endif
        }

    } while(true);
}
