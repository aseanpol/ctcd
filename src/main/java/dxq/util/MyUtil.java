package dxq.util;

import java.awt.geom.Point2D;
import java.io.*;
import java.math.BigDecimal;

import java.awt.geom.Path2D;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.lang.management.*;
import java.util.GregorianCalendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class MyUtil {
    public static final Logger utilsLog = LogManager.getLogger(MyUtil.class);
    private String brokerAddress, brokerUsername, brokerPassword, clientId, ctcTopic;
    private String amqpHost, amqpUsername, amqpPassword, amqpVirtualHost, amqpExchange, amqpRoutingKey, providerTaxCode;
    private int amqpPort;
    private static String rtsURL, gcURL;
    private String dbAddress, dbPort, dbUsername, dbPassword, dbSchema;
    private int qos;
    private int subscribeInterval, publishInterval;
    private boolean forwardEnable;
    private static Path2D.Float path2D;
    private String photosPath;

    public MyUtil(){
        try{
            File inputFile = new File("conf/config.xml");
            DocumentBuilderFactory fact = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = fact.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();

            Element root = doc.getDocumentElement();

            NodeList parentNodeList = root.getChildNodes();
            for(int i=0; i<parentNodeList.getLength(); i++){
                Node node = parentNodeList.item(i);
                if(node.getNodeType()==Node.ELEMENT_NODE) {
                    if(node.getNodeName().equalsIgnoreCase("mqtt")){
                        Element nod = (Element)node;
                        brokerAddress = nod.getElementsByTagName("broker").item(0).getTextContent();
                        brokerUsername = nod.getElementsByTagName("username").item(0).getTextContent();
                        brokerPassword = nod.getElementsByTagName("password").item(0).getTextContent();
                        qos = Integer.parseInt(nod.getElementsByTagName("qos").item(0).getTextContent());
                        clientId = nod.getElementsByTagName("clientId").item(0).getTextContent();
                        ctcTopic = nod.getElementsByTagName("ctc_topic").item(0).getTextContent();
                    }else if(node.getNodeName().equalsIgnoreCase("amqp")){
                        Element nod = (Element)node;
                        amqpHost = nod.getElementsByTagName("host").item(0).getTextContent();
                        amqpPort = Integer.parseInt(nod.getElementsByTagName("port").item(0).getTextContent());
                        amqpUsername = nod.getElementsByTagName("username").item(0).getTextContent();
                        amqpPassword = nod.getElementsByTagName("password").item(0).getTextContent();
                        amqpVirtualHost = nod.getElementsByTagName("virtual_host").item(0).getTextContent();
                        amqpExchange = nod.getElementsByTagName("exchange").item(0).getTextContent();
                        amqpRoutingKey = nod.getElementsByTagName("routing_key").item(0).getTextContent();
                        providerTaxCode = nod.getElementsByTagName("taxcode").item(0).getTextContent();
                    }else if(node.getNodeName().equalsIgnoreCase("rts_database")){
                        Element nod = (Element)node;
                        dbAddress = nod.getElementsByTagName("address").item(0).getTextContent();
                        dbPort = nod.getElementsByTagName("port").item(0).getTextContent();
                        dbUsername = nod.getElementsByTagName("username").item(0).getTextContent();
                        dbPassword = nod.getElementsByTagName("password").item(0).getTextContent();
                        dbSchema = nod.getElementsByTagName("schema").item(0).getTextContent();
                        rtsURL = "jdbc:mysql://" + dbAddress + ":" + dbPort + "/" + dbSchema +
                                "?user=" + dbUsername + "&password=" + dbPassword +
                                "&useUnicode=true&characterEncoding=utf8&useSSL=false&autoReconnect=true";
                    }else if(node.getNodeName().equalsIgnoreCase("gc_database")){
                        Element nod = (Element)node;
                        dbAddress = nod.getElementsByTagName("address").item(0).getTextContent();
                        dbPort = nod.getElementsByTagName("port").item(0).getTextContent();
                        dbUsername = nod.getElementsByTagName("username").item(0).getTextContent();
                        dbPassword = nod.getElementsByTagName("password").item(0).getTextContent();
                        dbSchema = nod.getElementsByTagName("schema").item(0).getTextContent();
                        gcURL = "jdbc:mysql://" + dbAddress + ":" + dbPort + "/" + dbSchema +
                                "?user=" + dbUsername + "&password=" + dbPassword +
                                "&useUnicode=true&characterEncoding=utf8&useSSL=false&autoReconnect=true";
                    }else if(node.getNodeName().equalsIgnoreCase("watchdog")){
                        Element nod = (Element)node;
                        subscribeInterval = Integer.parseInt(nod.getElementsByTagName("subscribe-interval").item(0).getTextContent());
                        publishInterval = Integer.parseInt(nod.getElementsByTagName("publish-interval").item(0).getTextContent());
                        photosPath = nod.getElementsByTagName("photos-path").item(0).getTextContent();
                    }/*else if(node.getNodeName().equalsIgnoreCase("forward_server")){
                        Element nod = (Element)node;
                        forwardEnable = Boolean.parseBoolean(nod.getElementsByTagName("enable").item(0).getTextContent());
                        forwardAddress = nod.getElementsByTagName("address").item(0).getTextContent();
                        forwardPort = Integer.parseInt(nod.getElementsByTagName("port").item(0).getTextContent());
                    }*/
                }
            }
        }catch(Exception ex) {
            utilsLog.info("Xu ly XML loi : "+ex,"info");
            ex.printStackTrace();
        }

		/*try{
			ds1=new BasicDataSource();
			ds1.setDriverClassName("com.mysql.jdbc.Driver");
			ds1.setUrl(gtsURL);
			ds1.setMinIdle(5);
			ds1.setMaxIdle(20);
			ds1.setMaxOpenPreparedStatements(50);
			ds2=new BasicDataSource();
			ds2.setDriverClassName("com.mysql.jdbc.Driver");
			ds2.setUrl(gcURL);
			ds2.setMinIdle(5);
			ds2.setMaxIdle(20);
			ds2.setMaxOpenPreparedStatements(50);
		}catch(Exception ex){
			utilsLog.info("dbcp error : "+ex,"error");
			ex.printStackTrace();
		}*/
//        loadVNPolygon();
    }

    public static Connection getRtsConnection() throws Exception {
        //return ds1.getConnection();
//        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
//        Connection con=DriverManager.getConnection(rtsURL);
        Connection con = DriverManager.getConnection(rtsURL);
        return con;
    }

    public static Connection getGcConnection() throws Exception {
        //return ds2.getConnection();
//        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        Connection con=DriverManager.getConnection(gcURL);
        return con;
    }

	/*public int update(String sql) {
		Connection con=null;
		Statement st=null;
		int row=-1;
		try{
			con=ds1.getConnection();
			st=con.createStatement();
			row=st.executeUpdate(sql);
		}catch(Exception ex){
			ex.printStackTrace();
			row=-1;
		}finally{
			try{
				if(st!=null) st.close();
				if(con!=null) con.close();
			}catch(Exception e){e.printStackTrace();}
		}
		return row;
	}*/

    //	public int getTcpPort(){
//		return tcpPort;
//	}
//    public int getQPort(){
//        return this.qPort;
//    }
//
//    public int getConcoxPort(){
//        return this.concoxPort;
//    }
//
//    public int getNasiaPort(){
//        return this.nasiaPort;
//    }
//
//    public int getYunxionPort(){
//        return this.yunxionPort;
//    }

    public String getBrokerAddress(){
        return this.brokerAddress;
    }

    public String getBrokerUsername(){
        return this.brokerUsername;
    }

    public String getBrokerPassword(){
        return this.brokerPassword;
    }

    public int getQoS(){
        return this.qos;
    }

    public String getClientId(){
        return this.clientId;
    }

    public String getCtcTopic(){
        return this.ctcTopic;
    }

    public String getAmqpHost(){
        return this.amqpHost;
    }

    public int getAmqpPort(){
        return this.amqpPort;
    }

    public String getAmqpUsername(){
        return this.amqpUsername;
    }

    public String getAmqpPassword(){
        return this.amqpPassword;
    }

    public String getAmqpVirtualHost(){
        return this.amqpVirtualHost;
    }

    public String getAmqpExchange(){
        return this.amqpExchange;
    }

    public String getAmqpRoutingKey(){
        return this.amqpRoutingKey;
    }

    public String getProviderTaxCode(){
        return this.providerTaxCode;
    }
//    public int getGotrackPort(){
//        return this.gotrackPort;
//    }

//    public String getGcMasterAddress(){
//        return gcmasterAddress;
//    }

//	public int getGcMasterPort(){
//		return gcmasterPort;
//	}

//	public int getGcMasterWdInterval(){
//		return gcmasterWdInterval;
//	}

//    public String getForwardAddress(){
//        return forwardAddress;
//    }
//
//    public int getForwardPort(){
//        return forwardPort;
//    }
//
//    public boolean isForwardEnable(){
//        return forwardEnable;
//    }
    public String getPhotosPath(){
    return this.photosPath;
}

//    public static String createPhotoFilePath(String imei){// throws Exception {
//        String path=System.getProperty("photo.path");;
//        //String imei="6101225537";
//        String dateFolder="",fileName="";
//        // create folder imei
//        path+="/"+imei;
//        File f=new File(path);
//        f.mkdir();
//        // create folder date
//        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
//        dateFolder=formatter.format(new Date());
//        dateFolder=dateFolder.substring(0,dateFolder.indexOf(" "));
//        path+="/"+dateFolder;
//        f=new File(path);
//        f.mkdir();
//        // Eg : fileName=2011-06-20 20-25-21.jpg
//        formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
//        fileName=formatter.format(new Date())+".jpg";
//        path+="/"+fileName;
//        return path;
//    }
//    public static String createPublishPhotoFilePath(String imei,String fileName){
//        String path=System.getProperty("publish.photo.path");;
//        String dateFolder="";//,fileName="";
//        // create folder imei
//        path+="/"+imei;
//        File f=new File(path);
//        f.mkdir();
//        // create folder date
//        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
//        dateFolder=formatter.format(new Date());
//        dateFolder=dateFolder.substring(0,dateFolder.indexOf(" "));
//        path+="/"+dateFolder;
//        f=new File(path);
//        f.mkdir();
//        // Eg : fileName=2011-06-20 20-25-21.jpg
//        path+="/"+fileName;
//        return path;
//    }

    public static int countMatches(String str, String sub) {
        if (isEmpty(str) || isEmpty(sub)) {
            return 0;
        }
        int count = 0;
        int idx = 0;
        while ((idx = str.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

	public static Point2D.Float getGPRMCPoint(String lati, String longi) {
		try{
			String lat = lati.substring(0, lati.length() - 1);
			String latDirection = lati.substring(lati.length() - 1);
			String lo = longi.substring(0, longi.length() - 1);
			String loDirection = longi.substring(longi.length() - 1);

			double latGrade = Double.parseDouble(lat.substring(0,
					lat.indexOf(".") - 2));
			double latDecimalGrade = Double.parseDouble(lat.substring(lat
					.indexOf(".") - 2)) / 60;
			double latitude = latGrade + latDecimalGrade;
			if (latDirection.equals("S"))
				latitude = -latitude;
			double longGrade = Double.parseDouble(lo.substring(0,
					lo.indexOf(".") - 2));
			double longDecimalGrade = Double.parseDouble(lo.substring(lo
					.indexOf(".") - 2)) / 60;
			double longitude = longGrade + longDecimalGrade;
			if (loDirection.equals("W"))
				longitude = -longitude;
			float la=BigDecimal.valueOf(latitude).setScale(5,BigDecimal.ROUND_HALF_UP).floatValue();
			float lon=BigDecimal.valueOf(longitude).setScale(5,BigDecimal.ROUND_HALF_UP).floatValue();
			//System.out.println(new Point2D.Float(la,lon));
			return new Point2D.Float(la,lon);
		}catch(Exception ex){
			ex.printStackTrace();
			return null;
		}
	}

    // return bearing of 2 point
    public static int bearing(float lat1, float lng1, float lat2, float lng2){
        double lati1,lati2,longi1,longi2,radBear,degBear;
        int angle;

        lati1 = Math.toRadians(lat1);
        longi1 = Math.toRadians(lng1);
        lati2 = Math.toRadians(lat2);
        longi2 = Math.toRadians(lng2);

        radBear = Math.atan2(Math.asin(longi2-longi1)*Math.cos(lati2),Math.cos(lati1)*Math.sin(lati2) - Math.sin(lati1)*Math.cos(lati2)*Math.cos(longi2-longi1));
        if(radBear<0) radBear+=Math.PI*2;
        degBear = Math.toDegrees(radBear);
        angle=((int)Math.round(degBear/3))*3;
        if(angle==360) angle=0;
        return angle;
    }

    // return distance in met
    public static int distance(float lat1, float lng1, float lat2, float lng2){
        int r=6371000;
        if(lat1==0 || lat2==0 || lng1==0 || lng2==0) return 0;
        double dLat = Math.toRadians(lat2-lat1);
        double dLng = Math.toRadians(lng2-lng1);
        double sindLat = Math.sin(dLat / 2);
        double sindLng = Math.sin(dLng / 2);
        double a = Math.pow(sindLat,2) + Math.pow(sindLng,2) * Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2));
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        double dist = r * c;
        return (int)dist;
    }

    private void loadVNPolygon(){
        Element root;
        boolean firstVertex=true;
        path2D=new Path2D.Float();
        try{
            File inputFile = new File("conf/polygon.xml");
            DocumentBuilderFactory fact = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = fact.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();
            root=doc.getDocumentElement();
            NodeList parentNodeList=root.getChildNodes();
            for(int i=0;i<parentNodeList.getLength();i++){
                Node node=parentNodeList.item(i);
                if(node.getNodeType()==Node.ELEMENT_NODE) {
                    if(node.getNodeName().startsWith("point")){
                        Element nod=(Element)node;
                        float lat=Float.parseFloat(nod.getElementsByTagName("lat").item(0).getTextContent());
                        float lng=Float.parseFloat(nod.getElementsByTagName("lng").item(0).getTextContent());
                        if(firstVertex){
                            path2D.moveTo(lat,lng);
                        }else{
                            path2D.lineTo(lat,lng);
                        }
                        firstVertex=false;
                    }
                }
            }
            System.out.println("Load polygon finish");
        }catch(Exception ex){
            utilsLog.info("Xu ly XML polygon loi : "+ex,"info");
        }
    }

    public static boolean checkPointInVN(float x,float y){
        return path2D.contains(x,y);
    }

    public static String getMemory(){
        int scale=1024*1024;
        long total=Runtime.getRuntime().totalMemory();
        total=total/scale;
        long free=Runtime.getRuntime().freeMemory();
        free=free/scale;
        long max=Runtime.getRuntime().maxMemory();
        max=max/scale;

        String str="Using/Total="+(total-free)+"/"+total+" MB. Max="+max+" MB";
        return str;
    }

    public static String[] getThreadInfo(){
        ThreadMXBean thread = ManagementFactory.getThreadMXBean();
        long[] threadIDs=thread.getAllThreadIds();
        String[] arr=new String[threadIDs.length];
        for(int i=0;i<threadIDs.length;i++){
            ThreadInfo info=thread.getThreadInfo(threadIDs[i]);
            arr[i]=info.toString();
        }
        return arr;
    }

    public static String unixTime2Date(long unixTime){
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        final String dateStr = Instant.ofEpochSecond(unixTime).atZone(ZoneId.of("GMT+7")).format(formatter);
        return dateStr;
    }

    // Quang add 28/12/22
    public static boolean isNewDay(long currentUnixTime, long dbUnixTime){
        // ngay hien tai
        Calendar cal = new GregorianCalendar();
        cal.setTimeInMillis(currentUnixTime * 1000);
        int currentDay = cal.get(Calendar.DAY_OF_MONTH);
        // ngay trong db
        cal.setTimeInMillis(dbUnixTime * 1000);
        int dbDay = cal.get(Calendar.DAY_OF_MONTH);

        return (currentDay==dbDay+1)? true:false;
    }
}