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

    private static final String TOPIC_A = "fileTest"; //for test

    private final byte[] fileNameBytes = {0x00, 0x09};
    private final byte[] finalBytes = {0x00, 0x11};
    int fileNmaeMessageNo;
    FileOutputStream fileOutputStream;

    int messageNo = 1;


    @KafkaListener(topics = TOPIC_A, groupId = "group_file", containerFactory = "kafkaListenerContainerFactory")
    public void consumeFile(ConsumerRecord<Integer, byte[]> record, Acknowledgment ack)//ack is for manual submit offset
    {

        byte[] receivedBytes = record.value();
        //get file name byte
        if (Arrays.equals(record.value(), fileNameBytes) && messageNo == record.key())
        {
            fileNmaeMessageNo = record.key() + 1;
            System.out.println("get fileNameBytes");


            messageNo++;
            //submit offset
            ack.acknowledge();
            return;
        }

        //get file name
        if(record.key() == fileNmaeMessageNo && messageNo == record.key())
        {
            String fileName = new String(receivedBytes);
            try
            {
//                fileOutputStream = new FileOutputStream("D:\\NtustMaster\\First\\Project\\CIMFORCE\\testFile\\output\\" + fileName);
                fileOutputStream = new FileOutputStream("D:\\NtustMaster\\" + fileName);
                System.out.println("get fileNameInBytes");

                //submit offset
                messageNo++;
                ack.acknowledge();
                return;
            } catch (FileNotFoundException e)
            {
                e.printStackTrace();
            }
        }

        //consumer over
        if (Arrays.equals(record.value(), finalBytes) && messageNo == record.key())
        {
            System.out.println("Consume over");
            try
            {
                fileOutputStream.close();
                //submit offset
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

        //run here when consuming file bytes
        try
        {
            if(messageNo == record.key())
            {
                fileOutputStream.write(receivedBytes);
//                System.out.println("write Bytes");
                //submit offset
                messageNo++;
                ack.acknowledge();
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }



//    //TestFileTransfer
//    private byte[] concatenateByteArray(byte[] a, byte[] b)
//    {
//
//
//        byte[] c = new byte[a.length + b.length];
//        System.arraycopy(a, 0, c, 0, a.length);
//        System.arraycopy(b, 0, c, a.length, b.length);
//        return c;
//    }
}
