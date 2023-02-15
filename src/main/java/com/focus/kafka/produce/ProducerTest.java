package com.focus.kafka.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerTest {


    @Test
    public void producer() throws ExecutionException, InterruptedException {

        String topic = "test2";
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //kafka  持久化数据的MQ  数据-> byte[]，不会对数据进行干预，双方要约定编解码
        //kafka是一个app：：使用零拷贝  sendfile 系统调用实现快速数据消费
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.ACKS_CONFIG, "-1");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);

        //现在的producer就是一个提供者，面向的其实是broker，虽然在使用的时候我们期望把数据打入topic

        /*
        test2,2partition,三种商品，每种商品有线性的3个ID,相同的商品最好去到一个分区里
         */


        while (true) {
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "item" + j, "val" + i);
                    Future<RecordMetadata> send = producer.send(record);

                    RecordMetadata rm = send.get();
                    int partition = rm.partition();
                    long offset = rm.offset();
                    System.out.println("key: " + record.key() + " val: " + record.value() + " partition: " + partition + " offset: " + offset);

                }
            }
        }


    }


    /**
     * key: item0 val: val0 partition: 1 offset: 0
     * key: item1 val: val0 partition: 0 offset: 0
     * key: item2 val: val0 partition: 1 offset: 1
     * key: item0 val: val1 partition: 1 offset: 2
     * key: item1 val: val1 partition: 0 offset: 1
     * key: item2 val: val1 partition: 1 offset: 3
     * key: item0 val: val2 partition: 1 offset: 4
     * key: item1 val: val2 partition: 0 offset: 2
     * key: item2 val: val2 partition: 1 offset: 5
     * key: item0 val: val0 partition: 1 offset: 6
     * key: item1 val: val0 partition: 0 offset: 3
     * key: item2 val: val0 partition: 1 offset: 7
     * key: item0 val: val1 partition: 1 offset: 8
     * key: item1 val: val1 partition: 0 offset: 4
     * key: item2 val: val1 partition: 1 offset: 9
     * key: item0 val: val2 partition: 1 offset: 10
     * key: item1 val: val2 partition: 0 offset: 5
     * key: item2 val: val2 partition: 1 offset: 11
     * key: item0 val: val0 partition: 1 offset: 12
     * key: item1 val: val0 partition: 0 offset: 6
     * key: item2 val: val0 partition: 1 offset: 13
     * key: item0 val: val1 partition: 1 offset: 14
     * key: item1 val: val1 partition: 0 offset: 7
     * key: item2 val: val1 partition: 1 offset: 15
     * key: item0 val: val2 partition: 1 offset: 16
     * key: item1 val: val2 partition: 0 offset: 8
     * key: item2 val: val2 partition: 1 offset: 17
     * key: item0 val: val0 partition: 1 offset: 18
     * key: item1 val: val0 partition: 0 offset: 9
     * key: item2 val: val0 partition: 1 offset: 19
     * key: item0 val: val1 partition: 1 offset: 20
     * key: item1 val: val1 partition: 0 offset: 10
     * key: item2 val: val1 partition: 1 offset: 21
     * key: item0 val: val2 partition: 1 offset: 22
     *
     * **/
}