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
 * 异步发送消息, 事件回调
 * @author xiaopantx
 */
public class ASyncProducer {

    /**
     * nameserver 地址
     */
    public static final String NAME_SRV = "192.168.1.100:9876";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer_group");
        producer.setNamesrvAddr(MQConst.NAME_SRV);
        producer.start();
        // 10000条消息全部确认成功之后, 返回
        CountDownLatch latch = new CountDownLatch(100);
        long start = System.currentTimeMillis();
        for (int i = 1; i <= 10000; i++) {
            Message message = new Message("mytopic_01", (i + "msg").getBytes(StandardCharsets.UTF_8));
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    String jsonResult = SendResult.encoderSendResultToJson(sendResult);
                    System.out.println("jsonResult = " + jsonResult);
                    latch.countDown();
                }
                @Override
                public void onException(Throwable e) {
                    // 消息投递失败~ 设置一些处理逻辑, 重新投递
                    String errorMsg = e.getMessage();
                    System.out.println("消息发送失败: " + errorMsg);
                }
            });
        }
        // 等待broker消息确认
        latch.await();
        // 关闭发送端
        producer.shutdown();
        long end = System.currentTimeMillis();
        // 4797ms
        System.out.println("发送10000条异步消息耗时: " + (end - start) + "ms");
    }
}
