package io.emqx.mqtt;

import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;

/**
 * MQTT 示例
 */
public class MqttExample implements MqttCallback {
    public static void main(String[] args) {
        String broker = "tcp://ip:1883";// 连接地址
        int qos = 0;// 消息质量
        String action = "publish";// 操作类型
        String topic = "test/topic";// 主题
        String message = "Hello MQTT";// 消息内容
        String clientId = MqttClient.generateClientId();// 客户端ID
        boolean cleanSession = true;// 是否清除会话
        String userName = "front";// 用户名
        String password = "123456";// 密码
        for (int i = 0; i < args.length; i++) {// 解析参数
            if (args[i].length() == 2 && args[i].startsWith("-")) {// 解析参数
                char arg = args[i].charAt(1);// 参数
                if (arg == 'h') {// 帮助
                    help();// 输出帮助信息
                    return;
                }

                if (i == args.length - 1 || args[i + 1].charAt(0) == '-') {// 缺少参数
                    System.out.println("Missing value for argument: " + args[i]);// 输出错误信息
                    help();// 输出帮助信息
                    return;
                }
                switch (arg) {// 设置参数
                    case 'b':
                        broker = args[++i];// 设置 MQTT 服务器地址
                        break;
                    case 'a':
                        action = args[++i];// 设置操作类型（publish/subscribe）
                        break;
                    case 't':
                        topic = args[++i];// 设置主题
                        break;
                    case 'q':
                        qos = Integer.parseInt(args[++i]);// 设置 QoS
                        break;
                    case 'c':
                        cleanSession = Boolean.parseBoolean(args[++i]);// 设置清除会话
                        break;
                    case 'u':
                        userName = args[++i];// 设置用户名
                        break;
                    case 'z':
                        password = args[++i];// 设置密码
                        break;
                    default:
                        System.out.println("Unknown argument: " + args[i]);// 输出错误信息
                        help();// 输出帮助信息
                        return;
                }
            } else {
                System.out.println("Unknown argument: " + args[i]);// 输出错误信息
                help();// 输出帮助信息
                return;
            }
        }

        if (!action.equals("publish") && !action.equals("subscribe")) {// 检查操作类型是否有效
            System.out.println("Invalid action: " + action);// 输出错误信息
            help();// 输出帮助信息
            return;
        }
        if (qos < 0 || qos > 2) {// 检查 QoS 是否有效
            System.out.println("Invalid QoS: " + qos);// 输出错误信息
            help();// 输出帮助信息
            return;
        }

        MqttExample sample = new MqttExample(broker, clientId, cleanSession, userName, password);// 创建实例
        try {
            if (action.equals("publish")) {// 检查操作类型
                sample.publish(topic, qos, message.getBytes());// 发布消息
            } else {
                sample.subscribe(topic, qos);// 订阅主题
            }
        } catch (MqttException e) {
            e.printStackTrace();// 输出异常信息
        }
    }

    private MqttClient client;// 客户端
    private String brokerUrl;// 服务器地址
    private MqttConnectOptions options;// 连接选项
    private boolean clean;// 是否清除会话
    private String password;// 密码
    private String userName;// 用户名

    public MqttExample(String brokerUrl, String clientId, boolean cleanSession, String userName, String password) {
        this.brokerUrl = brokerUrl;// 服务器地址
        this.clean = cleanSession;// 是否清除会话
        this.password = password;// 密码
        this.userName = userName;// 用户名

        options = new MqttConnectOptions();// 连接选项
        options.setCleanSession(clean);// 设置是否清除会话
        if (userName != null) {// 检查用户名和密码是否有效
            options.setUserName(this.userName);// 设置用户名
        }
        if (password != null) {// 检查用户名和密码是否有效
            options.setPassword(this.password.toCharArray());// 设置密码
        }

        try {
            client = new MqttClient(this.brokerUrl, clientId);// 创建客户端
            client.setCallback(this);// 设置回调
        } catch (MqttException e) {
            e.printStackTrace();// 打印异常信息
            log(e.toString());
            System.exit(1);// 退出JVM
        }
    }

    public void subscribe(String topicName, int qos) throws MqttException {// 订阅主题

        client.connect(options);// 连接服务器
        log("Connected to " + brokerUrl + " with client ID " + client.getClientId());// 打印连接信息

        client.subscribe(topicName, qos);// 订阅主题
        log("Subscribed to topic: " + topicName + " qos " + qos);// 打印订阅信息

        try {
            System.in.read();// 等待键盘输入
        } catch (IOException e) {
        }

        client.disconnect();// 断开连接
        log("Disconnected");
    }

    public void publish(String topicName, int qos, byte[] payload) throws MqttException {// 发布消息
        client.connect(options);// 连接服务器
        log("Connected to " + brokerUrl + " with client ID " + client.getClientId());// 打印连接信息

        MqttMessage message = new MqttMessage(payload);// 创建消息
        message.setQos(qos);// 设置QoS

        client.publish(topicName, message);// 发布消息
        log("Published to topic \"" + topicName + "\" qos " + qos);// 打印发布信息
        client.disconnect();// 断开连接
        log("Disconnected");// 打印断开连接信息
    }

    private void log(String message) {
        System.out.println(message);
    }


    public void connectionLost(Throwable throwable) {// 连接丢失
        log("Connection lost: " + throwable);// 打印连接丢失信息
        System.exit(1);// 退出
    }

    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {// 消息接收
        log("Received message:\n" +
                "Topic: " + s + "\t" +
                "Message: " + mqttMessage.toString()
        );
    }

    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {// 消息发送完成
    }

    private static void help() {
        System.out.println(
                "Args:\n" +
                        "-h Help information\n" +
                        "-b MQTT broker url [default: tcp://broker.emqx.io:1883]\n" +
                        "-a publish/subscribe action [default: publish]\n" +
                        "-u Username [default: emqx]\n" +
                        "-z Password [default: public]\n" +
                        "-c Clean session [default: true]\n" +
                        "-t Publish/Subscribe topic [default: test/topic]\n" +
                        "-q QoS [default: 0]"
        );
    }
}
