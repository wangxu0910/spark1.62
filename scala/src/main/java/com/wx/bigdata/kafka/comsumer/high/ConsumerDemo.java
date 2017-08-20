package com.wx.bigdata.kafka.comsumer.high;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2017/6/24.
 */
public class ConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("group.id","group1");
        properties.put("zookeeper.connect","hadoop.wangxu:2181/kafka");
        //默认的offset默认的提交间隔由60s改为10s
        properties.put("auto.commit.interval.ms","10000");

        ConsumerConfig config = new ConsumerConfig(properties);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

        //读取数据，key为topic名称，value为读取该topic的线程数量，一般情况下，一个分区对应一个线程，
        //一个线程可以处理多个分区的数据，但是，一个分区只能被一个线程处理，如果线程数量大于分区数量
        //部分线程不会处理数据
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("topic1",2);
        Decoder<String> keyDecoder = new StringDecoder(new VerifiableProperties());
        Decoder<String> valueDecoder = keyDecoder;
        //每个topic对应一个list对象，list的KafkaStream数量就是topicCountMap对应的线程数量
        Map<String, List<KafkaStream<String,String>>> stringListMap = consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        final List<KafkaStream<String,String>> streamList = stringListMap.get("topic1");
        System.out.println("strams的数量"+streamList.size());

        for (int i=0;i<streamList.size();i++){
            final KafkaStream kafkaStream = streamList.get(i);
            ExecutorService executor = Executors.newCachedThreadPool();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    ConsumerIterator iter = kafkaStream.iterator();
                    while (iter.hasNext()) {
                        MessageAndMetadata messageAndMetadata = iter.next();
                        String threadName = Thread.currentThread().getName();
                        System.out.println("线程名称:" + threadName + ";Topic名称:" + messageAndMetadata.topic() + ";分区id:" + messageAndMetadata.partition() + ";当前数据偏移量:" + messageAndMetadata.offset() + ";key=" + messageAndMetadata.key() + ";value=" + messageAndMetadata.message());
                    }
                }
            },"thread-"+i);
        }

    }
}
