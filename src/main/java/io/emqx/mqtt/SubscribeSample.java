package io.emqx.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

// 订阅消息
public class SubscribeSample {
    public static void main(String[] args) {

        String broker = "tcp://ip::1883";
        String topic = "test/test";
        String username = "end";
        String password = "123456";
        String clientid = "subscribe_client";
        int qos = 0;

        try {
            MqttClient client = new MqttClient(broker, clientid, new MemoryPersistence());// 创建客户端
            // 设置连接参数
            MqttConnectOptions options = new MqttConnectOptions();// 创建连接参数
            options.setCleanSession(true);// 设置是否清空session,这里如果设置为false表示服务器会保留客户端的连接记录，这里设置为true表示每次连接到服务器都是新的连接
            options.setUserName(username);// 设置用户名
            options.setPassword(password.toCharArray());// 设置密码
            options.setConnectionTimeout(60);// 设置超时时间
            options.setKeepAliveInterval(60);// 设置心跳包发送间隔
            // 设置回调
            client.setCallback(new MqttCallback() {// 设置回调

                public void connectionLost(Throwable cause) {// 连接丢失
                    System.out.println("connectionLost: " + cause.getMessage());// 连接丢失后，重新连接
                }

                public void messageArrived(String topic, MqttMessage message) {// 消息到达
                    System.out.println("topic: " + topic);// 订阅的主题
                    System.out.println("Qos: " + message.getQos());// 消息的Qos
                    System.out.println("message content: " + new String(message.getPayload()));// 消息的内容

                }

                public void deliveryComplete(IMqttDeliveryToken token) {// 消息发送完成
                    System.out.println("deliveryComplete---------" + token.isComplete());// 完成状态
                }

            });
            client.connect(options);// 连接服务器
            client.subscribe(topic, qos);// 订阅主题
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
