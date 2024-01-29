package io.emqx.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MqttFileTransferSample {
    private final static int SEGMENT_SIZE = 1024 * 10;// 10KB
    private final static String CLIENT_ID_PREFIX = "emqx-file-transfer-";// 客户端ID前缀
    private final static int QOS = 1;// QoS 1
    private MqttClient client;// MQTT client
    private String username;
    private String password;
    public MqttFileTransferSample(String username, String password) {// 构造函数
        this.username = username;
        this.password = password;
    }

    public void initClient(String brokerUrl, String clientId) {// 初始化客户端
        try {
            client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());// 创建MQTT客户端
            MqttConnectOptions options = new MqttConnectOptions();// 设置MQTT的连接属性
            options.setUserName(username);// 设置用户名
            options.setPassword(password.toCharArray());// 设置密码
            client.connect(options);// 连接到MQTT服务器
            if (!client.isConnected()) {// 判断是否连接成功
                log("Fail to connected " + brokerUrl + " with client ID " + client.getClientId());// 连接失败
                System.exit(1);// 退出程序
            }
            log("Connected " + brokerUrl + " with client ID " + client.getClientId());// 连接成功

        } catch (MqttException e) {// 捕获异常
            e.printStackTrace();// 打印异常信息
            log(e.toString());// 打印异常信息
            System.exit(1);// 退出程序
        }

    }

    public void transferFile(String filePath, String host) {// 传输文件
        File file = new File(filePath);// 创建文件对象

        if (!file.exists()) {// 判断文件是否存在
            System.out.println("File does not exist : " + filePath);// 文件不存在
            System.exit(1);// 退出程序
        }

        String clientId = CLIENT_ID_PREFIX + MqttClient.generateClientId();// 创建客户端ID
        initClient(host, clientId);// 初始化客户端

        try {
            // Use file checksum as file_id
            String fileChecksum = calculateChecksum(filePath);// 计算文件的校验和

            String fileId = fileChecksum;// 创建文件ID
            long fileSize = file.length();// 创建文件大小

            // The client device publishes init command topic.
            // The payload of the message contains the file metadata, including the file name, size, and checksum.
            pubInitCommand(fileId, file.getName(), fileSize, fileChecksum);// 发布初始化命令

            //  The client sends consecutive segment commands
            //  Each segment command carries a chunk of the file data at the specified offset.
            pubSegmentCommands(file, fileId);// 发布分片命令

            // The client sends finish command
            pubFinishCommand(fileId, fileSize);// 发布结束命令

        } catch (Exception e) {
            System.out.println("msg " + e.getMessage());// 输出异常信息
            System.out.println("log " + e.getLocalizedMessage());// 输出异常信息
            System.out.println("cause " + e.getCause());// 输出异常信息
            System.out.println("excep " + e);// 输出异常信息
            e.printStackTrace();// 输出异常信息
        }finally {
            try {
                client.disconnect();// 断开连接
                client.close();// 关闭连接
            } catch (MqttException e) {
                throw new RuntimeException(e);// 抛出运行时异常
            }
        }

    }

    private void pubFinishCommand(String fileId, long fileSize) throws MqttException {// 发布结束命令
        String finishTopic = "$file/" + fileId + "/fin/" + fileSize;// 发布结束命令
        publishMessage(finishTopic, QOS, "".getBytes());// 发布结束命令
        log("File transfer finished.");// 输出日志信息
    }


    public void pubInitCommand(String fileId, String fileName, long fileSize, String fileChecksum) throws MqttException {// 发布初始化命令
        String initTopic = "$file/" + fileId + "/init";// 发布初始化命令

        String initMsg = initMsg(fileName, fileSize, fileChecksum);// 发布初始化命令
        System.out.println("Init Msg : "+initMsg);// 输出初始化命令
        publishMessage(initTopic, QOS, initMsg.getBytes());// 发布初始化命令
        log("File transfer session initialized.");// 输出日志信息
    }

    public void pubSegmentCommands(File file, String fileId) throws IOException, MqttException {// 发布文件分段命令
        log("Send file segment start =>");// 输出日志信息
        FileChannel fileChannel = FileChannel.open(file.toPath());// 打开文件
        // Read the file and publish segments
        int offset = 0;// 偏移量
        while (true) {
            // Read a segment from the file
            int capacity = (int) file.length() - offset;// 文件大小
            capacity = Math.min(SEGMENT_SIZE, capacity);// 文件大小
            ByteBuffer buffer = ByteBuffer.allocate(capacity);// 分配缓冲区
            fileChannel.read(buffer);// 读取文件
            buffer.flip();// 翻转缓冲区

            // Publish the segment
            String segmentTopic = "$file/" + fileId + "/" + offset;// 发布文件分段命令

            offset += buffer.limit();// 偏移量
            publishMessage(segmentTopic, QOS, buffer.array());// 发布文件分段命令

            // Check if the end of the file has been reached
            if (buffer.limit() < SEGMENT_SIZE) {// 检查是否到达文件末尾
                break;
            }
        }
        fileChannel.close();// 关闭文件
        log("Send file segment end ");
    }


    private static String initMsg(String fileName, long fileSize, String fileChecksum) {// 初始化文件信息
        return "{\"name\":\"" + fileName + "\",\"size\":" + fileSize + ",\"checksum\":\"" + fileChecksum + "\"}";// 初始化文件信息
    }


    public static String calculateChecksum(String filePath) throws IOException, NoSuchAlgorithmException {// 计算文件校验和
        // Get the MessageDigest instance for the checksum algorithm
        MessageDigest md5 = MessageDigest.getInstance("SHA-256");// 获取SHA-256算法实例

        // Get the file data
        File file = new File(filePath);// 获取文件

        byte[] fileData = Files.readAllBytes(Paths.get(file.getPath()));// 读取文件

        // Update the hash with the file data
        md5.update(fileData);// 更新校验和

        // Convert the hash to a string
        return new BigInteger(1, md5.digest()).toString(16);// 转换为十六进制字符串
    }


    public void publishMessage(String topicName, int qos, byte[] payload) throws MqttException {// 发布消息
        if (!client.isConnected()) {// 检查是否已连接
            System.out.println("client is unconnectd");// 未连接
            System.exit(1);// 退出程序
        }

        MqttMessage message = new MqttMessage(payload);// 创建消息
        message.setQos(qos);// 设置消息质量
        client.publish(topicName, message);// 发布消息
        log("Published to topic \"" + topicName + "\" qos " + qos + " size:" + payload.length);// 发布消息
    }

    private static void log(String message) {// 打印日志
        System.out.println(message);
    }

    public static void main(String[] args) {
        String userName = "emqx";// 用户名
        String password = "public";// 密码
        String broker = "tcp://broker.emqx.io:1883";// 服务器地址
        String filePath = "";// 文件路径

        for (int i = 0; i < args.length; i++) {// 遍历参数
            if (args[i].length() == 2 && args[i].startsWith("-")) {// 检查参数格式
                char arg = args[i].charAt(1);// 获取参数
                if (arg == 'h') {// 帮助
                    help();
                    return;
                }

                if (i == args.length - 1 || args[i + 1].charAt(0) == '-') {
                    System.out.println("Missing value for argument: " + args[i]);
                    help();
                    return;
                }
                switch (arg) {
                    case 'b':
                        broker = args[++i];
                        break;
                    case 'f':
                        filePath = args[++i];
                        break;
                    case 'u':
                        userName = args[++i];
                        break;
                    case 'z':
                        password = args[++i];
                        break;
                    default:
                        System.out.println("Unknown argument: " + args[i]);
                        help();
                        return;
                }
            } else {
                System.out.println("Unknown argument: " + args[i]);
                help();
                return;
            }
        }

        if(filePath == null || filePath.length() <=0){
            System.out.println("The argument f is required ");
            help();
            return;
        }

        System.out.println("Args => broker:"+broker+" filePath:"+filePath+" userName="+userName+" password:"+password);
        new MqttFileTransferSample(userName,password).transferFile(filePath, broker);
    }


    private static void help() {
        System.out.println(
                "Args:\n" +
                        "-h Help information\n" +
                        "-b MQTT broker url [default: tcp://broker.emqx.io:1883]\n" +
                        "-f The absolute path of the file to be uploaded [Required]\n" +
                        "-u Username [default: emqx]\n" +
                        "-z Password [default: public]\n"
        );
    }

}
