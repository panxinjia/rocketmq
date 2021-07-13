import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class Consumer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group");
        consumer.setNamesrvAddr(MQConst.NAME_SRV);

        consumer.subscribe("mytopic_01", "*");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                byte[] body = msg.getBody();
                String content = new String(body, StandardCharsets.UTF_8);
                System.out.println("收到消息: " + content);
            }
            // 消费成功
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });


        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.start();

        System.out.println("消息接收完成~");
    }
}
