package com.anderson.kafka.springbootkafkaconsumer.listener;

import com.anderson.kafka.springbootkafkaconsumer.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
//fortest
@Service
public class KafkaConsumer
{
    private final byte[] finalBytes = {0x00, 0x11};
    FileOutputStream fileOutputStream;

    {
        try
        {
            fileOutputStream = new FileOutputStream("D:\\testFile\\output\\test_550MB.exe");
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    @KafkaListener(topics = "Kafka_Example", groupId = "group_id")
    public void consume(String message)
    {
        System.out.println("Consumed message: " + message);
    }

    @KafkaListener(topics = "Kafka_Example_json", groupId = "group_json", containerFactory = "userKafkaListenerFactory")
    public void consumeJson(User user)
    {
        System.out.println("Consumed JSON Message: " + user);
    }

    @KafkaListener(topics = "Kafka_Example_file", groupId = "group_file", containerFactory = "fileKafkaListenerContainerFactory")
    public void consumeFile(ConsumerRecord<Integer, byte[]> record)
    {
        System.out.println("Consumed file Message: key " + record.key() + ", value " + Arrays.toString(record.value()));
        byte[] receivedBytes = record.value();
        if(Arrays.equals(record.value(), finalBytes))
        {
            System.out.println("Consume over");
            try
            {
                fileOutputStream.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
            return;
        }
        try {
            fileOutputStream.write(receivedBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private static final String TOPIC = "Kafka_Example_json";

    //TestFileTransfer
    private byte[] concatenateByteArray(byte[] a, byte[] b)
    {


        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }
}
