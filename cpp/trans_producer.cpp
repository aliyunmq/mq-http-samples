//#include <iostream>
#include <fstream>
#ifdef _WIN32
#include <windows.h>
#include <process.h>
#else
#include "pthread.h"
#endif
#include "mq_http_sdk/mq_client.h"

using namespace std;
using namespace mq::http::sdk;


const int32_t pubMsgCount = 4;
const int32_t halfCheckCount = 3;

void processCommitRollError(AckMessageResponse& bdmResp, const std::string& messageId) {
    if (bdmResp.isSuccess()) {
        cout << "Commit/Roll Transaction Suc: " << messageId << endl;
        return;
    }
    const std::vector<AckMessageFailedItem>& failedItems =
        bdmResp.getAckMessageFailedItem();
    for (std::vector<AckMessageFailedItem>::const_iterator iter = failedItems.begin();
            iter != failedItems.end(); ++iter)
    {
        cout << "Commit/Roll Transaction ERROR: " << iter->errorCode
            << "  " << iter->receiptHandle << endl;
    }
}

#ifdef WIN32
unsigned __stdcall consumeHalfMessageThread(void *arg)
#else
void* consumeHalfMessageThread(void *arg)
#endif
{
    MQTransProducerPtr transProducer = *(MQTransProducerPtr*)(arg);
    int count = 0;
    do {
        std::vector<Message> halfMsgs;
        try {
            // 长轮询消费消息
            // 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
            transProducer->consumeHalfMessage(
                    1,//一次最多消费1条(最多可设置为16条)
                    3,//长轮询时间3秒（最多可设置为30秒） 
                    halfMsgs 
                    );
        } catch (MQServerException& me) {
            if (me.GetErrorCode() == "MessageNotExist") {
                cout << "No half message to consume! RequestId: " + me.GetRequestId() << endl;
                continue;
            }
            cout << "Request Failed: " + me.GetErrorCode() + ".RequestId: " + me.GetRequestId() << endl;
        }
        if (halfMsgs.size() == 0) {
            continue;
        }

        cout << "Consume Half: " << halfMsgs.size() << " Messages!" << endl;
        // 处理事务半消息
        std::vector<std::string> receiptHandles;
        for (std::vector<Message>::iterator iter = halfMsgs.begin();
                iter != halfMsgs.end(); ++iter)
        {
            cout << "MessageId: " << iter->getMessageId()
                << " PublishTime: " << iter->getPublishTime()
                << " Tag: " << iter->getMessageTag()
                << " Body: " << iter->getMessageBody()
                << " FirstConsumeTime: " << iter->getFirstConsumeTime()
                << " NextConsumeTime: " << iter->getNextConsumeTime()
                << " ConsumedTimes: " << iter->getConsumedTimes() 
                << " Properties: " << iter->getPropertiesAsString() 
                << " Key: " << iter->getMessageKey() << endl;

            int32_t consumedTimes = iter->getConsumedTimes();
            const std::string propA = iter->getProperty("a");
            const std::string handle = iter->getReceiptHandle();
            AckMessageResponse bdmResp;
            if (propA == "1") {
                cout << "Commit msg.." << endl;
                transProducer->commit(handle, bdmResp);
                count++;
            } else if(propA == "2") {
                if (consumedTimes > 1) {
                    cout << "Commit msg.." << endl;
                    transProducer->commit(handle, bdmResp);
                    count++;
                } else {
                    cout << "Commit Later!!!" << endl;
                }
            } else if(propA == "3") {
                cout << "Rollback msg.." << endl;
                transProducer->rollback(handle, bdmResp);
                count++;
            } else {
                transProducer->commit(handle, bdmResp);
                cout << "Unkown msg.." << endl;
            }
            // 如果Commit/Rollback时超过了NextConsumeTime的时间则会失败
            processCommitRollError(bdmResp, iter->getMessageId());
        }

    } while(count < halfCheckCount);

#ifdef WIN32
	return 0;
#else
	return NULL;
#endif
}

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
    // 您在控制台创建的 Consumer ID(Group ID)
    string groupId = "${GROUP_ID}";

    MQTransProducerPtr transProducer;
    if (instanceId == "") {
        transProducer = mqClient.getTransProducerRef(topic, groupId);
    } else {
        transProducer = mqClient.getTransProducerRef(instanceId, topic, groupId);
    }

    // 客户端需要有一个线程或者进程来消费没有确认的事务消息
    // 示例这里启动一个线程来检查没有确认的事务消息
#ifdef WIN32
	HANDLE thread;
	unsigned int threadId;
	thread = (HANDLE)_beginthreadex(NULL, 0, consumeHalfMessageThread, &transProducer, 0, &threadId);
#else
	pthread_t thread;
	pthread_create(&thread, NULL, consumeHalfMessageThread, static_cast<void *>(&transProducer));
#endif

    try {
        for (int i = 0; i < pubMsgCount; i++)
        {
            PublishMessageResponse pmResp;
            TopicMessage pubMsg("Hello, mq, trans_msg!");
            pubMsg.putProperty("a",std::to_string(i));
            pubMsg.setMessageKey("ImKey");
            pubMsg.setTransCheckImmunityTime(10);
            transProducer->publishMessage(pubMsg, pmResp);
            cout << "Publish mq message success. Topic:" << topic 
                << ", msgId:" << pmResp.getMessageId() 
                << ", bodyMD5:" << pmResp.getMessageBodyMD5()
                << ", Handle:" << pmResp.getReceiptHandle() << endl;

            if (i == 0) {
                // 发送完处理了业务逻辑,可直接Commit/Rollback
                // 如果Commit/Rollback时超过了TransCheckImmunityTime则会失败
                AckMessageResponse bdmResp;
                transProducer->commit(pmResp.getReceiptHandle(), bdmResp);
                processCommitRollError(bdmResp, pmResp.getMessageId());
            }
        }
    } catch (MQServerException& me) {
        cout << "Request Failed: " + me.GetErrorCode() << ", requestId is:" << me.GetRequestId() << endl;
    } catch (MQExceptionBase& mb) {
        cout << "Request Failed: " + mb.ToString() << endl;
    }

#ifdef WIN32
	WaitForSingleObject(thread, INFINITE);
	CloseHandle(thread);
#else
	pthread_join(thread, NULL);
#endif

    return 0;
}
