package dxq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.*;

import dxq.util.*;
import dxq.tracker.*;

public class Service implements MqttCallbackExtended{
    public static final Logger serviceLog = LogManager.getLogger(Service.class);
    private MyUtil util = null;
    private MemoryPersistence persistence = new MemoryPersistence();
    private MqttClient client;
    private MqttConnectOptions opt;
    private Channel channel;
    private TrackerAdapter adapter = null;
//    private static final String LOCATION_MSG = "gps";
//    private static final String FIRMWARE_MSG = "fw";
//    private static final String EVENT_MSG = "event";
//    private static final String STATUS_MSG = "status";
    String supplier1 = "ctc";
    String supplier2 = "ttc";
    String revision1 = "ctc1";
    String revision2 = "ctc2";
    String revision3 = "ttc1";
    String revision4 = "ttc2";

    public Service(){
        util = new MyUtil();
        opt = new MqttConnectOptions();
        opt.setCleanSession(true);
        opt.setUserName(util.getBrokerUsername());
        opt.setPassword(util.getBrokerPassword().toCharArray());
        opt.setAutomaticReconnect(true);
        opt.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        connectMqttCloud();
        connectAmqpDRVN(util);
//        int minLevel = 0;
//        int maxLevel = 700;
//        int minValue = 5;
//        int maxValue = 60;
//        float sampleValue = (float)(maxValue - minValue)/(maxLevel - minLevel);
//        System.out.println("float="+sampleValue);
    }

    private void connectMqttCloud() {
        serviceLog.info("Connecting mqtt cloud "+util.getBrokerAddress()+" . . .","info");
        try {
            client=new MqttClient(util.getBrokerAddress(),util.getClientId(),persistence);
            client.setCallback(this);
            client.connect(opt);
        }catch(MqttException ex) {
//            serviceLog.error(ex.toString(),"error");
            ex.printStackTrace();
        }
    }

    //--------------------------------------------------
    // subscribe
    public void subscribeCTCDevices() {
        String topic = util.getCtcTopic();
        try {
            if(client.isConnected()) {
                client.subscribe(topic);
                serviceLog.info("subscribe : "+topic,"info");
                System.out.println("Subscribe CTC topic");
            }else{
                System.out.println("Connection be lost when subscribe");
            }
        }catch(Exception ex) {
            serviceLog.error(ex.toString(),"error");
            ex.printStackTrace();
        }
    }
    //--------------------------------------------------
    // implements MqttCakkbackExtended
    public void connectionLost(Throwable cause) {
        serviceLog.info("connectionLost","info");
        System.out.println("Connection be lost");
    }

    public void connectComplete(boolean complete,String uri) {
        System.out.println("connect="+complete+",uri="+uri);
        serviceLog.info("Connect to "+uri+" : "+complete,"info");
        subscribeCTCDevices();
//        if(complete) subscribeList(switchBeans.getSubscribeSwitchList());
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
        serviceLog.info("deliveryComplete","info");
    }

    public synchronized void messageArrived(String topic, MqttMessage message) throws Exception{
//        System.out.println(topic+"--"+message);
        String[] top = topic.split("/");
        if((supplier1.equals(top[0]) || supplier2.equals(top[0])) && (revision1.equals(top[1]) || revision2.equals(top[1]) || revision3.equals(top[1]) || revision4.equals(top[1]))){
            String imei = top[2];
            String msgType = top[3];
//            System.out.println(imei + "," + msgType);
//            byte[] b = message.getPayload();
//            System.out.println("payload length=" + b.length);
//            Thread th = new TrackerAdapter(imei, msgType, message.toString(), this.channel, this.util);
            Thread th = new TrackerAdapter(imei, msgType, message.getPayload(), this.channel, this.util);
            th.start();
        }else{
            System.out.println("Syntax not matched");
        }
    }

    private void connectAmqpDRVN(MyUtil util) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(util.getAmqpHost());
        factory.setUsername(util.getAmqpUsername());
        factory.setPassword(util.getAmqpPassword());
        factory.setPort(util.getAmqpPort());
        factory.setVirtualHost(util.getAmqpVirtualHost());

        try{
            com.rabbitmq.client.Connection connection = factory.newConnection();
            this.channel = connection.createChannel();
            System.out.println("channel=" + channel);
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
