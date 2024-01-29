package io.emqx.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class PublishSample {

    public static void main(String[] args) {

        String broker = "tcp://ip:1883";
        // TLS/SSL
        // String broker = "ssl://broker.emqx.io:8883";

        String topic = "mqtt/test";// 主题
        String username = "end";// 用户名
        String password = "123456";// 密码
        String clientid = "publish_client";// 客户端ID
        String content = "Hello MQTT12";// 消息内容

        int qos = 0;// 质量等级

        try {
            MqttClient client = new MqttClient(broker, clientid, new MemoryPersistence());// 创建客户端
            // 连接参数
            MqttConnectOptions options = new MqttConnectOptions();// 创建连接参数
            // 设置用户名和密码
            options.setUserName(username);
            options.setPassword(password.toCharArray());
            options.setKeepAliveInterval(60);// 设置心跳包发送间隔
            options.setConnectionTimeout(60);// 设置连接超时时间
            // 连接
            client.connect(options);// 连接
            // 创建消息并设置 QoS
            MqttMessage message = new MqttMessage(content.getBytes());// 创建消息
            message.setQos(qos);// 设置 QoS
            // 发布消息
            client.publish(topic, message);// 发布消息
            System.out.println("Message published");
            System.out.println("topic: " + topic);
            System.out.println("message content: " + content);
            // 断开连接
            client.disconnect();
            // 关闭客户端
            client.close();
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }

    }
}
