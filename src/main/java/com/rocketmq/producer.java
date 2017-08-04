package com.rocketmq;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * Created by Admin on 2017-08-02.
 */
public class producer {
    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new
                DefaultMQProducer("please_rename_unique_group_name");
        producer.setNamesrvAddr("192.168.137.102:9876");
        //Launch the instance.
        producer.start();
        for (int i = 0; i < 100; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("hello" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " +
                            i).getBytes() /* Message body */
            );
            //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
//    public static void main(String[] args){
//        DefaultMQProducer producer = new DefaultMQProducer("Producer");
//        producer.setNamesrvAddr("192.168.137.102:9876");
//        try {
//            producer.start();
//
//            Message msg = new Message("BenchmarkTest",
//                    "push",
//                    "1",
//                    "Just for test.".getBytes());
//
//            SendResult result = producer.send(msg);
//            System.out.println("id:" + result.getMsgId() +
//                    " result:" + result.getSendStatus());
//
//            msg = new Message("BenchmarkTest",
//                    "push",
//                    "2",
//                    "Just for test.".getBytes());
//
//            result = producer.send(msg);
//            System.out.println("id:" + result.getMsgId() +
//                    " result:" + result.getSendStatus());
//
//            msg = new Message("PullTopic",
//                    "pull",
//                    "1",
//                    "Just for test.".getBytes());
//
//            result = producer.send(msg);
//            System.out.println("id:" + result.getMsgId() +
//                    " result:" + result.getSendStatus());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }finally{
//            producer.shutdown();
//        }
//    }
}
