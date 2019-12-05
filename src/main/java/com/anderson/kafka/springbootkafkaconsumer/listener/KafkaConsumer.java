package com.anderson.kafka.springbootkafkaconsumer.listener;

import com.anderson.kafka.springbootkafkaconsumer.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

//fortest
@Service
public class KafkaConsumer
{

//    private static final String TOPIC_A = "Kafka_Example_file_A"; //for file experiment
    private static final String TOPIC_A = "fileTest"; //for test
    private static final String TOPIC_B = "Kafka_Example_file_B";
    private static final String TOPIC_C = "Kafka_Example_file_C";

    private final byte[] fileNameBytes = {0x00, 0x09};
    private final byte[] fileContentBytes = {0x00, 0x10};
    private final byte[] finalBytes = {0x00, 0x11};
    int fileNmaeMessageNo;
    FileOutputStream fileOutputStream;

    int messageNo = 1;

    {
//        try
//        {
//            fileOutputStream = new FileOutputStream("D:\\testFile\\output\\test_5000MB.exe");
//        } catch (FileNotFoundException e)
//        {
//            e.printStackTrace();
//        }
    }

//    @KafkaListener(topics = "Kafka_Example", groupId = "group_id")
//    public void consume(String message)
//    {
//        System.out.println("Consumed message: " + message);
//    }
//
//    @KafkaListener(topics = "Kafka_Example_json", groupId = "group_json", containerFactory = "userKafkaListenerFactory")
//    public void consumeJson(User user)
//    {
//        System.out.println("Consumed JSON Message: " + user);
//    }

    @KafkaListener(topics = TOPIC_A, groupId = "group_file", containerFactory = "fileKafkaListenerContainerFactory")
    public void consumeFile(ConsumerRecord<Integer, byte[]> record, Acknowledgment ack)
    {
//        System.out.println("messageNo : " + messageNo + "   record.key : " + record.key());
//        System.out.println("Consumed file Message: key " + record.key() + ", value " + Arrays.toString(record.value()));

        byte[] receivedBytes = record.value();
//        try
//        {
//            Thread.sleep(5000);
//        }
//        catch (InterruptedException e)
//        {
//            e.printStackTrace();
//        }
//        if(record.key() == 1)
//            System.out.println("Consumed file Message: key " + record.key() + ", value " + Arrays.toString(record.value()));
//        if(record.key() == 2)
//            System.out.println("Consumed file Message: key " + record.key() + ", value " + Arrays.toString(record.value()));


        //get file name byte
        if (Arrays.equals(record.value(), fileNameBytes) && messageNo == record.key())
        {
            fileNmaeMessageNo = record.key() + 1;
            System.out.println("get fileNameBytes");

            //提交offset
            messageNo++;
            ack.acknowledge();
            return;
        }

        //get file name
        if(record.key() == fileNmaeMessageNo && messageNo == record.key())
        {
            String fileName = new String(receivedBytes);
            try
            {
                fileOutputStream = new FileOutputStream("D:\\NtustMaster\\First\\Project\\CIMFORCE\\testFile\\output\\" + fileName);
                System.out.println("get fileNameInBytes");

                //提交offset
                messageNo++;
                ack.acknowledge();
                return;
            } catch (FileNotFoundException e)
            {
                e.printStackTrace();
            }
        }


        if (Arrays.equals(record.value(), finalBytes) && messageNo == record.key())
        {
            System.out.println("Consume over");
            try
            {
                fileOutputStream.close();
                //提交offset
                messageNo = 1;
                ack.acknowledge();
                try
                {
                    Thread.sleep(10000);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
            return;
        }


        try
        {
            if(messageNo == record.key())
            {
                fileOutputStream.write(receivedBytes);
//                System.out.println("write Bytes");
                //提交offset
                messageNo++;
                ack.acknowledge();
            }
        } catch (IOException e)
        {
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
