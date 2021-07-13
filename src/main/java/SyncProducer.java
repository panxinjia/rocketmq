import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

/**
 * 同步发送
 * @author xiaopantx
 */
public class SyncProducer {

    public static final String NAME_SRV = "192.168.1.100:9876";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer_group");
        producer.setNamesrvAddr(NAME_SRV);
        producer.start();

        long start = System.currentTimeMillis();
        for(int i = 1; i <= 10000; i++) {
            Message message = new Message("mytopic_01", (i + " msg").getBytes(StandardCharsets.UTF_8));
            // 同步发送, 等待Broker的ack送达, 才发送下一条
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult.getSendStatus().name());
        }
        producer.shutdown();
        long end = System.currentTimeMillis();
        //50142ms
        System.out.println("发送10000条同步消息耗时: " + (end - start) + "ms");
    }

}
