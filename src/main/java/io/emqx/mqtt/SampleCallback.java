package io.emqx.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class SampleCallback implements MqttCallback {
    public void connectionLost(Throwable cause) {// 连接丢失后，一般在这里面进行重连
        System.out.println("connection lost：" + cause.getMessage());
    }

    public void messageArrived(String topic, MqttMessage message) {// 收到消息后，一般在这里处理消息
        System.out.println("Received message: \n  topic：" + topic + "\n  Qos：" + message.getQos() + "\n  payload：" + new String(message.getPayload()));
    }

    public void deliveryComplete(IMqttDeliveryToken token) {// 消息发送后，一般在这里进行消息的确认
        System.out.println("deliveryComplete");
    }


}
