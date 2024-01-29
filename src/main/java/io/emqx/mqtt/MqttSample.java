package io.emqx.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;


public class MqttSample {
    public static void main(String[] args) {
        String topic = "test/topic";// 订阅主题
        String content = "Hello World";// 发送内容
        int qos = 2;// 质量等级
        String broker = "tcp://ip:1883";// 服务器地址
        String clientId = MqttClient.generateClientId();// 客户端ID
        MemoryPersistence persistence = new MemoryPersistence();// 存储方式
        MqttConnectOptions connOpts = new MqttConnectOptions();// 连接选项
        connOpts.setUserName("end");// 用户名
        connOpts.setPassword("123456".toCharArray());// 密码
        try {
            MqttClient client = new MqttClient(broker, clientId, persistence);// 创建客户端
            client.setCallback(new SampleCallback());// 设置回调

            System.out.println("Connecting to broker: " + broker);// 连接服务器
            client.connect(connOpts);// 连接服务器
            System.out.println("Connected to broker: " + broker);// 连接成功
            client.subscribe(topic, qos);// 订阅主题
            System.out.println("Subscribed to topic: " + topic);// 订阅成功
            MqttMessage message = new MqttMessage(content.getBytes());// 创建消息
            message.setQos(qos);// 设置质量等级
            client.publish(topic, message);// 发布消息
            System.out.println("Message published");// 发布成功
            client.disconnect();// 断开连接
            System.out.println("Disconnected");// 断开连接
            client.close();// 关闭客户端
            System.exit(0);// 退出
        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());// 异常原因
            System.out.println("msg " + me.getMessage());// 获取异常原因
            System.out.println("loc " + me.getLocalizedMessage());// 获取本地化消息
            System.out.println("cause " + me.getCause());// 原因
            System.out.println("excep " + me);// 打印异常
            me.printStackTrace();// 打印异常堆栈
        }
    }

}
