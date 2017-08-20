package com.wx.bigdata.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2017/6/24.
 */
public class producerDemo {
//    private static final char[] words = "qwertyuiopasdfghjklzxcvbnm".toCharArray();
private static final char[] words = "abc".toCharArray();
    private static final int wordsLength = words.length;
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list","hadoop.wangxu:9092,hadoop.wangxu:9093");
        properties.put("request.required.acks","0");
        properties.put("producer.type","sync");
        properties.put("serializer.class","kafka.serializer.StringEncoder");//序列化机制，一般给定，因为默认DefaultEncoder序列化
        // 实现机制是要求传入key/value类型必须是byte数值

        properties.put("partitioner.class","com.wx.bigdata.kafka.producer.PartitionerDemo");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        final Producer<String,String> producer = new Producer<String, String>(producerConfig);

        final Random random = new Random(System.currentTimeMillis());
        ExecutorService executor = Executors.newCachedThreadPool();
        int executorCount = 3;
        CountDownLatch countDownLatch = new CountDownLatch(executorCount);
        for (int i=0;i<executorCount;i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        int events = random.nextInt(100)+10;
                        for (int k=0;k<events;k++){
                            if (k!=0 && k%100==0){
                                System.out.println(Thread.currentThread().getName()+"发送数据"+k+"条");
                            }
                            String key = "key_" + random.nextInt(100);
                            StringBuilder sb = new StringBuilder();
                            String value = "";
                            for (int j=0;j<random.nextInt(5);j++){
                                sb.append(words[random.nextInt(wordsLength)]);
                            }
                            value = sb.toString();
                            KeyedMessage<String,String> message = new KeyedMessage<String, String>("topic2",key,value);
                            producer.send(message);
                        }
                        System.out.println(Thread.currentThread().getName()+"总共发送数据"+events+"条");
                    } catch (Exception e){
                        e.printStackTrace();
                    } finally {
                        countDownLatch.countDown();
                    }
                }
            },"thread-"+i);
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executor.shutdown();
        System.out.println("进行关闭操作");
        producer.close();
    }
}
