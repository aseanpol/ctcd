// sau khi INSERT INTO device_log -> cap nhat lastInsertID vao tracker.setLogID()

package dxq;

import java.awt.*;
import java.awt.font.TextAttribute;
import java.awt.image.BufferedImage;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.lang.Math;
import java.text.AttributedString;
import java.util.*;

import dxq.tracker.*;
import dxq.util.*;
import gov.mt.ufms.UfmsProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import com.rabbitmq.client.Channel;

import javax.imageio.ImageIO;
import java.io.*;

public class TrackerAdapter extends Thread{
    public static final Logger adapterLog= LogManager.getLogger(TrackerAdapter.class);
    private MyUtil util;
    private String imei = "";
    private String str = "";
    private String trackerType = "CTC1";
    private long unixTime = 0;
    private String msgType = "";
    private Channel channel;
    private byte[] payload = {};

//    public TrackerAdapter(String imei, String msgType, String str, Channel channel, MyUtil util) {
//        this.imei = imei;
//        this.msgType = msgType;
//        this.str = str;
//        this.channel = channel;
//        this.util = util;
//    }

    public TrackerAdapter(String imei, String msgType, byte[] payload, Channel channel, MyUtil util) {
        this.imei = imei;
        this.msgType = msgType;
//        this.str = str;
        this.payload = payload;
        this.channel = channel;
        this.util = util;

        if(payload.length < 256) str = new String(payload);
    }

    public void run(){
        // parsing
        long unixTime = 0;
        String driver = "", license = "";
        float lat = 0, lng = 0;
        int speed = 0, sdStatus = 0;
        boolean overSpeed = false, power = false, sd = true, door = false, isOvertime = false;
        int fuelRaw = 0;
        int angle = 0, distance = 0;

        int tempValue = 0;
        boolean aircon = false;
        boolean truck = false;
        boolean di2 = false;
        int drivingElapse = 0;
        int drivingStretch = 0;
//        int dayStretch = 0;

        // xu ly msgtype : co 4 msgType : location, event, command, qcvn
        // consumer chi xu ly 3 msg : location, event, qcvn
        // timeStamp, lat, lng, speed, acc, door, fuel, angle, distance, driver_name, driver_license, isOverSpeed, drivingElapse, isSessionOvertime, dayStretch
        if(msgType.equals(CTCTracker.LOCATION)){
            try{
                String[] items=str.split(",");
//            String version="",cmd;
                unixTime = Long.parseLong(items[0]);
                lat = Float.parseFloat(items[1]);
                lng = Float.parseFloat(items[2]);
                speed = Integer.parseInt(items[3]);

                power = items[4].equals("1") ? true:false;
//                door = items[5].equals("1") ? true:false;
                sd = items[5].equals("1") ? true:false;// dùng sdStatus thay thế cho door
                fuelRaw = Integer.parseInt(items[6]);
                angle = Integer.parseInt(items[7]);
                distance = Integer.parseInt(items[8]);
                driver=items[9];
                license=items[10];
                overSpeed = (Integer.parseInt(items[11]) == 1)? true:false;
                drivingElapse = Integer.parseInt(items[12]);
                isOvertime = items[13].equals("1") ? true:false;
//                dayStretch = Integer.parseInt(items[14]);// Quang disable 16/10/22 : khong lay tu device
            }catch(Exception ex){
//            this.writeQChannelLog("Exception in parsingMessage, msg = " + ex.getMessage());
                ex.printStackTrace();
                return;
            }

            Tracker t = this.validateIMEI(imei, Tracker.CTCTRACKER);
            if(t instanceof CTCTracker){
                // IMEI hợp lệ
                CTCTracker ctc = (CTCTracker)t;
                ctc.setTime(unixTime);
                ctc.setIMEI(imei);
//                ctc.setType("CTC1");// Quang edit 22/12/22
                ctc.setLat(lat);
                ctc.setLng(lng);
                ctc.setSpeed(speed);
//                if(speed>=ctc.getSpeedLimit()) ctc.setOverSpeed(true);

                ctc.setPower(power);
                ctc.setSD(sd);
                if(lat==0 || lng==0) ctc.setGps(false);

                if(ctc.isFuelEnable()){
                    // Quang edit 10/12/22
                    ctc.setFuelRaw(fuelRaw);
                    ctc.calibFuel(fuelRaw);
                    System.out.println("calib device id="+ctc.getID());
                }
                if(ctc.isTempEnable()){
                    ctc.setTemp(tempValue);
                    boolean overTemp = (tempValue >= ctc.getTempLimit())? true:false;
                    ctc.setOverTemp(overTemp);
                }
                if(ctc.isAirconEnable()){
                    // tam thoi test aircon=truck=power
                    ctc.setAircon(aircon);
                }
                if(ctc.isTruckEnable()){
                    ctc.setTruck(truck);
                }
                // dvr chưa xử lý
//                ctc.setDoor(door);
                ctc.setDi2(false);

                ctc.setAngle(angle);
//                ctc.setDistance(distance);// Quang disable 26/08/22 : chuyen qua setDistance trong prepareLocation

                ctc.setDriverName(driver);
                ctc.setDriverLicense(license);
                ctc.setOverSpeed(overSpeed);
                // Quang add 30/05/22 : drivingElapse và isOvertime
                ctc.setDrivingElapse(drivingElapse);
                ctc.setDrivingOvertime(isOvertime);

                // Quang add 16/10/22 : kiem tra neu fixGPS
                if(ctc.isFixGPS()){
                    if(!power){
                        // khong co ACC : xem nhu stop
                        ctc.setSpeed(0);
                        ctc.setDistance(0);
                    }
                }
            }else{
                // không có IMEI
                System.out.println("ctc is null : " + imei);
            }
            if(t!=null) {
                // nếu lost gps thì chỉ update time và event, driver
                if(!t.isGps()){
                    this.updateTime(t);
//                    System.out.println("update time");
                }else{
                    this.prepareLocation(t);
                    this.updateLocation(t);
//                    System.out.println("update location");
                }
                // update mileage table
                this.updateMileage(t);
                // forward AMQP
                if(t.isND91()){
                    boolean success = this.sendAMQP(util.getAmqpExchange(), util.getAmqpRoutingKey(), util.getProviderTaxCode(), t.getTaxCode(), t.getDriverLicense(), t.getNumberPlate(), t.getTime(), t.getLat(), t.getLng(), t.getSpeed(), t.getAngle(), t.getPower());
                    updateAMQPMileageLog(success, t.getID(), t.getMileageID());
//                    System.out.println("is ND91");
                }
//                System.out.println("imei=" + imei + ", distance=" + t.getDistance() + ", dayStretch=" + t.getDayStretch() + ", is fixGPS=" + t.isFixGPS());
//                System.out.println("imei="+ imei + ",unixTime="+util.unixTime2Date(unixTime)+", lat=" + lat + ", lng=" +lng + ",speed=" + speed + ", rawFuel=" + fuelRaw + ", fuelValue=" + t.getFuel()+",driver="+driver+",license="+license);
            }else{
//                System.out.println("is null, imei=" + imei);
            }
//            System.out.println(str);
        }else if(msgType.equals(CTCTracker.EVENT)){
            JsonParser parser = new JsonParser();
            JsonElement root = parser.parse(str);
            String deviceId = "", type = "", result = "", param = str;
            int time = 0;
            if (root.isJsonObject()) {
                JsonObject obj = root.getAsJsonObject();
                deviceId = obj.get("id").getAsString();
                time = obj.get("time").getAsInt();// nhieu msg khong co time
                type = obj.get("type").getAsString();

                result = obj.has("result")? obj.get("result").getAsString():"";

                if(type.equals("version")){
                    // Quang add 07/01/23 : version
                    result = obj.has("fw")? obj.get("fw").getAsString():"";
                }else if(type.equals("network")){
                    // Quang add 19/01/23 : network
                    String phoneNumber = obj.has("phone_number")? obj.get("phone_number").getAsString():"";
                    String operator = obj.has("operator")? obj.get("operator").getAsString():"";
                    int rssi = obj.has("rssi")? obj.get("rssi").getAsInt():0;
                    String imsi = obj.has("imsi")? obj.get("imsi").getAsString():"";
                    String iccid = obj.has("iccid")? obj.get("iccid").getAsString():"";
                    param = phoneNumber + "," + operator + "," + rssi + "," + imsi + "," + iccid;
                }
                this.updateEvent(deviceId, type, result, param, time);
            }
        }else if(msgType.equals(CTCTracker.QCVN)){
            // phân chia ra các type
            JsonParser parser = new JsonParser();
            JsonElement root = parser.parse(str);
            String deviceId = "", type = "", data = "", result = "", param = str;
            int time = 0;
            if (root.isJsonObject()) {
                JsonObject obj = root.getAsJsonObject();
                deviceId = obj.get("id").getAsString();
                time = obj.get("time").getAsInt();
                type = obj.get("type").getAsString();
                // phân chia các loại type : speed, overspeed, . . .
                if(type.equals("speed")){
                    // loại 5 L05.TXT
                    data = obj.get("data").getAsString();
                    this.updateQCVNSpeedPerSec(deviceId, time, data);
//                    System.out.println(data);
                }else if(type.equals("overspeed")){
                    // loại 5 OSPEED.TXT
                    String driverName = obj.get("driver_name").getAsString();
                    String driverLicense = obj.get("driver_license").getAsString();
                    int averageSpeed = obj.get("average_speed").getAsInt();
                    int speedLimit = obj.get("speed_limit").getAsInt();
                    String coordinate = obj.get("coordinate").getAsString();
                    int stretch = obj.get("stretch").getAsInt();
                    String desc = obj.get("desc").getAsString();
                    this.updateQCVNOverSpeed(deviceId, time, driverName, driverLicense, averageSpeed, speedLimit, coordinate, stretch, desc);
                }else if(type.equals("stop")){
                    // loai 3 L03.TXT
                    int startTime = obj.get("start_time").getAsInt();
                    String driverName = obj.get("driver_name").getAsString();
                    String driverLicense = obj.get("driver_license").getAsString();
                    // chuoi data giong y như record trong L03.TXT : <270522:08:37:21,108.169975,16.063414,1>
                    data = obj.get("data").getAsString();
                    String str = data.replace("<","");
                    str = str.replace(">","");
                    String[] arr = str.split(",");
                    String coordinate = arr[1] + "," + arr[2];
                    int elapse = Integer.parseInt(arr[3]);
                    // luu startTime vao receiveTime trong DB
                    this.updateQCVNPark(deviceId, startTime, driverName, driverLicense, coordinate, elapse, data);
                }else if(type.equals("driving")){
                    // loại 2 L02.TXT
                    // chuỗi data giống y như record trong L02.TXT : <290522:86101110195,86101110195,10:56:32,108.169937,16.063393,10:59:32,108.169930,16.063385>
                    drivingElapse = obj.get("elapse").getAsInt();
                    drivingStretch = obj.get("stretch").getAsInt();
//                    int drivingTimeLimit = obj.get("driving_time_limit").getAsInt();
                    int drivingTimeLimit = 14400;// tam thoi cho 14400
                    isOvertime = obj.get("is_ot").getAsBoolean();
                    int drivingStartTime = obj.get("start").getAsInt();
                    int drivingEndTime = obj.get("end").getAsInt();

                    data = obj.get("data").getAsString();
                    String str = data.replace("<","");
                    str = str.replace(">","");
                    String[] arr = str.split(",");
                    String startCoordinate = arr[3] + "," + arr[4];
                    String endCoordinate = arr[6] + "," + arr[7];
                    String driverName = arr[0];
                    String driverLicense = arr[1];
                    driverName = driverName.substring(driverName.indexOf(":")+1);
                    this.updateQCVNDriving(deviceId, time, driverName, driverLicense, drivingElapse, drivingStretch, drivingTimeLimit, isOvertime, drivingStartTime, startCoordinate, drivingEndTime, endCoordinate, data);
                }else if(type.equals("driver_operation")){
                    // cac event cua RFID : login, logout, luu action=rfid, khong luu type=driving_operation
                    String action = obj.get("action").getAsString();
                    result = obj.get("session").getAsString();
                    this.updateEvent(deviceId, action, result, param, time);
                }
            }
        }else if(msgType.equals(CTCTracker.PHOTO)){
            if(payload.length<100) return;// neu <100 byte thi msg error
            Tracker t = this.validateImageIMEI(imei, Tracker.CTCTRACKER);
            if(!(t instanceof CTCTracker)) {
                System.out.println("device " + imei + " is null, not process image");
                return;
            }
            // neu khong cam3Enable hoac khong phai ND91 thi khong xu ly
            if(!t.isCam3Enable() || !t.isND91()){
                System.out.println("device photo "+ imei + " khong enable cam3");
                return;
            }
            BufferedOutputStream bos = null;
            try {
                String param = new String(Arrays.copyOfRange(payload, payload.length - 100, payload.length));
                int semiColonIndex = param.lastIndexOf(";");
                if (semiColonIndex > 0) {
                    param = param.substring(semiColonIndex + 1);
                    // payload duoc cat bo phan ;param
                    payload = Arrays.copyOfRange(payload, 0, payload.length - param.length() - 1);

                    String[] arr = param.split(",");
                    unixTime = Long.parseLong(arr[0]);
                    lat = Float.parseFloat(arr[1]);
                    lng = Float.parseFloat(arr[2]);
                    speed = Integer.parseInt(arr[3]);
                    driver = arr[4];
                    license = arr[5];
                    int frameSize = Integer.parseInt(arr[6]);
                    String dateTime = MyUtil.unixTime2Date(unixTime);

                    String str1 = t.getNumberPlate() + ", " + dateTime;
                    String str2 = driver;
                    String str3 = lat+", "+lng+", "+speed+" km/h";

                    t.setTime(unixTime);
                    t.setLat(lat);
                    t.setLng(lng);
                    t.setSpeed(speed);
                    t.setDriverName(driver);
                    t.setDriverLicense(license);

                    // lay GEO : Quang edit 10/03/23 -> tach rieng ra method
//                    Connection geoCon = null;
//                    CallableStatement cs = null;
//                    String address = "";
//                    try {
//                        geoCon = util.getGcConnection();
//                        cs = geoCon.prepareCall("{?=call getGoogleGeo(?,?)}");
//                        cs.setFloat(2, t.getLat());// lat
//                        cs.setFloat(3, t.getLng());// lng
//                        cs.registerOutParameter(1, Types.NVARCHAR);// address
//                        cs.execute();
//                        address = cs.getString(1);
//                        cs.close();
//                        geoCon.close();
//                    }catch(Exception ex) {
//                        ex.printStackTrace();
//                        this.writeAdapterLog("Exception in photo, getAddress, msg = " + ex.getMessage());
//                    }
                    t.setAddress(getMyGeo(t.getLat(), t.getLng()));

                    t.setPhoto3Path(unixTime + ".jpg");
//                    String folderName = System.getProperty("user.dir") + "/images";
                    String folderName = util.getPhotosPath() + "/" + t.getID();
                    File folder = new File(folderName);
                    if (!folder.exists()) {
                        // neu chua co folder thi create folder
                        boolean success = folder.mkdir();
                        System.out.println("Create folder " + folderName + " success : " + success);
                        if (!success) {
                            // neu tao folder khong duoc thi return
                            System.out.println("Create folder error, imei=" + imei);
                            return;
                        }
                        // phan quyen folder deviceID
                        if(MyUtil.isLinux()){
                            Runtime.getRuntime().exec("chown " + MyUtil.chown + " " + folderName);
                        }else{}
                    }
                    String filePath = folderName + "/" + t.getPhoto3Path();
//                    System.out.println("param=" + unixTime + "," + lat + "," + lng + "," + speed + "," + driver + "," + license + "," + dateTime);

                    // co duoc mang byte payload -> xu ly image
                    BufferedImage img = addTextToImage(payload,str1, str2, str3);
                    if(img != null){
                        ImageIO.write(img, "jpg", new File(filePath));
                        img.flush();

                        // phan quyen chown cho file unixTime.jpg
                        if(MyUtil.isLinux()){
                            Runtime.getRuntime().exec("chown " + MyUtil.chown + " " + filePath);
                        }else{
                        }
                        // luu photo3Path vao bang photo
//                        updatePhoto3Path(t.getID(), t.getPhoto3Path());
//                        updatePhotoPath(t.getID(), 3, lat, lng, speed, unixTime, t.getPhoto3Path(), t.getAddress());
                        updatePhotoPath(t, 3);
                        System.out.println("save photo success : " + filePath + ", frame size=" + frameSize);
                    }
//                    System.out.println("payload length=" + payload.length + ",frameSize=" + frameSize + ",destination array length=" + payload.length);
                }
            }catch(Exception ex){
                ex.printStackTrace();
            }finally{
                try{
                    if(bos != null) bos.close();
                }catch(Exception e){}
            }
        }
    }

    public Tracker validateIMEI(String imei, int trackerType) {
        Tracker tracker = null;
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "";

        sql = "SELECT device_id, name, power_id, fuel_enable, tank_capac, fuel_delta, " +
                "fuel_id, temp_enable, temp_limit, aircon_enable, aircon_id, " +
                "truck_enable, truck_id, dvr_enable, dvr_id, door_id, log_id, " +
                "day_stretch, day_elapse, fix_gps, nd91, vin, speed_limit, speed_vio_id FROM device " +
                "WHERE imei='" + imei + "' LIMIT 1";
        boolean exist = false;
        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            rs = st.executeQuery(sql);

            if(rs.next()) {
                switch(trackerType){
                    case Tracker.CTCTRACKER:
                        tracker = new CTCTracker("CTC1");
                        break;
                }
                int deviceID = rs.getInt("device_id");
                String name = rs.getString("name");
                long power_id = rs.getLong("power_id");
                boolean fuel_enable = rs.getBoolean("fuel_enable");
                int tank_capac = rs.getInt("tank_capac");
                int fuel_delta = rs.getInt("fuel_delta");
                long fuel_id = rs.getLong("fuel_id");
                boolean temp_enable = rs.getBoolean("temp_enable");
                int temp_limit = rs.getInt("temp_limit");
                boolean aircon_enable = rs.getBoolean("aircon_enable");
                long aircon_id = rs.getLong("aircon_id");
                boolean truck_enable = rs.getBoolean("truck_enable");
                long truck_id = rs.getLong("truck_id");
                boolean dvr_enable = rs.getBoolean("dvr_enable");
                long dvr_id = rs.getLong("dvr_id");
                long door_id = rs.getLong("door_id");

                long logID = rs.getLong("log_id");
                int day_stretch = rs.getInt("day_stretch");
                int dayMovingElapse = rs.getInt("day_elapse");
                boolean fixGPS = rs.getBoolean("fix_gps");
                boolean nd91 = rs.getBoolean("nd91");
                int speed_limit = rs.getInt("speed_limit");
                long speedVioID = rs.getLong("speed_vio_id");
                String numberPlate = rs.getString("vin");

                st.close();
                con.close();

                tracker.setID(deviceID);
                tracker.setName(name);
                tracker.setIMEI(imei);
                tracker.setLogID(logID);

                tracker.setPowerId(power_id);
                tracker.setFuelEnable(fuel_enable);
                tracker.setTankCapac(tank_capac);
                tracker.setFuelDelta(fuel_delta);
                tracker.setFuelId(fuel_id);
                tracker.setTempEnable(temp_enable);
                tracker.setTempLimit(temp_limit);
                tracker.setAirconEnable(aircon_enable);
                tracker.setAirconId(aircon_id);
                tracker.setTruckEnable(truck_enable);
                tracker.setTruckId(truck_id);
                tracker.setDvrEnable(dvr_enable);
                tracker.setDvrId(dvr_id);
                tracker.setDayStretch(day_stretch);
                tracker.setDayMovingElapse(dayMovingElapse);
                tracker.setFixGPS(fixGPS);
                tracker.setND91(nd91);
                tracker.setSpeedLimit(speed_limit);
                tracker.setSpeedVioID(speedVioID);
                // Quang add 17/02/23
                tracker.setNumberPlate(numberPlate);
                exist = true;
            }else{
                // chua biet IMEI
//                System.out.println("chua khai bao IMEI = " + imei);
                this.writeAdapterLog("chua khai bao IMEI = " + imei);
                st.close();
                con.close();
                return null;
            }

            if(exist) {
                // update tracker type
                con = util.getRtsConnection();
//                st = con.createStatement();

                // Quang edit 22/12/22 : tam thoi disable version
//                sql = "UPDATE device SET tracker_type='" + tracker.getType() + "' " +
//                        "WHERE device_id=" + tracker.getID();
//                int rowCount = st.executeUpdate(sql);
//                st.close();

                // neu fuelEnable thi check calib

                if(tracker.isFuelEnable()){
                    TreeMap<Integer, Integer> calibMap = new TreeMap<Integer, Integer>();
                    sql = "SELECT * FROM calib WHERE device_id=" + tracker.getID();
                    st = con.createStatement();
                    rs = st.executeQuery(sql);
                    while(rs.next()) {
                        long calibID = rs.getLong("calib_id");
                        int sensorLevel = rs.getInt("sensor_level");
                        int fuelValue = rs.getInt("fuel_value");

                        calibMap.put(sensorLevel, fuelValue);
//                        System.out.println(calibID+","+sensorLevel+","+fuelValue);
                    }
                    tracker.setCalibMap(calibMap);
                    st.close();
                }
                con.close();
            }

            if(exist) {
                // lay taxCode trong slave account
                String taxCode = "";
                con = util.getRtsConnection();
                st = con.createStatement();

                sql = "SELECT tax_code from slave_account INNER JOIN slave_group ON slave_account.slave_id=slave_group.slave_id " +
                        "WHERE slave_group.device_id=" + tracker.getID() + " ORDER BY slave_account.slave_id LIMIT 1";
                rs = st.executeQuery(sql);

                if(rs.next()) {
                    taxCode = rs.getString("tax_code");
                }
                st.close();
                con.close();
                tracker.setTaxCode(taxCode);
            }
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in validateIMEI, msg = " + ex.getMessage());
            exist = false;
        }
        return tracker;
    }

    // Quang add 04/03/23 : gan giong validateIMEI nhung chi lay 1 so field can thiet cho image/photo
    public Tracker validateImageIMEI(String imei, int trackerType) {
        Tracker tracker = null;
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "";

//        sql = "SELECT device_id, name, power_id, fuel_enable, tank_capac, fuel_delta, " +
//                "fuel_id, temp_enable, temp_limit, aircon_enable, aircon_id, " +
//                "truck_enable, truck_id, dvr_enable, dvr_id, door_id, log_id, " +
//                "day_stretch, day_elapse, fix_gps, nd91, vin, speed_limit, speed_vio_id FROM device " +
//                "WHERE imei='" + imei + "' LIMIT 1";
        sql = "SELECT device_id, name, cam1_enable, cam2_enable, cam3_enable, " +
                "log_id, nd91, vin FROM device WHERE imei='" + imei + "' LIMIT 1";
        boolean exist = false;
        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            rs = st.executeQuery(sql);

            if(rs.next()) {
                switch(trackerType){
                    case Tracker.CTCTRACKER:
                        tracker = new CTCTracker("CTC1");
                        break;
                }
                int deviceID = rs.getInt("device_id");
                String name = rs.getString("name");
                boolean cam1Enable = rs.getBoolean("cam1_enable");
                boolean cam2Enable = rs.getBoolean("cam2_enable");
                boolean cam3Enable = rs.getBoolean("cam3_enable");
                long logID = rs.getLong("log_id");
                boolean nd91 = rs.getBoolean("nd91");
                String numberPlate = rs.getString("vin");

                st.close();
                con.close();

                tracker.setID(deviceID);
                tracker.setName(name);
                tracker.setIMEI(imei);
                tracker.setLogID(logID);
                tracker.setCam1Enable(cam1Enable);
                tracker.setCam2Enable(cam2Enable);
                tracker.setCam3Enable(cam3Enable);

                // Quang add 17/02/23
                tracker.setNumberPlate(numberPlate);
                exist = true;
            }else{
                // chua biet IMEI
//                System.out.println("chua khai bao IMEI = " + imei);
                this.writeAdapterLog("chua khai bao IMEI = " + imei);
                st.close();
                con.close();
                return null;
            }
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in validateIMEI, msg = " + ex.getMessage());
            exist = false;
        }
        return tracker;
    }

    // lay thong tin log truoc do trong device_log -> tinh toan cac gia tri -> luu vao class Tracker
    // truoc khi thuc hien updateLocation
    public void prepareLocation(Tracker tracker) {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "";
        sql = "SELECT latitude, longitude, speed, begin_time, end_time " +
                "FROM device_log" + " WHERE log_id=" + tracker.getLogID();
        boolean exist = false;
        int elapse = 0;

        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            rs = st.executeQuery(sql);
            if(rs.next()) {
                // has record
                float prevLat = rs.getFloat("latitude");
                float prevLng = rs.getFloat("longitude");
                int prevSpeed = rs.getInt("speed");
                long beginTime = rs.getLong("begin_time");
                long endTime = rs.getLong("end_time");
                tracker.setPrevLat(prevLat);
                tracker.setPrevLng(prevLng);
                tracker.setPrevSpeed(prevSpeed);
                tracker.setPrevBeginTime(beginTime);// de tinh duration
//                int movingElapse = rs.getInt("moving_elapse");
//                tracker.setMovingElapse(movingElapse);
//                if(prevSpeed == 0 && tracker.getSpeed() == 0){
//                    // dang STOP
//                    elapse = (int)(tracker.getTime() - beginTime);
//                }else{
//                    // 3 cac truong hop khac : elapse giua 2 msg
//                    elapse = (int)(tracker.getTime() - endTime);
//                }
                elapse = (int)(tracker.getTime() - endTime);
                if(elapse < 0) elapse = 0;
                if(!tracker.isLegalTime()) elapse = 0;// Quang add 04/01/23
                exist = true;
            }
            st.close();
            con.close();

            tracker.setElapse(elapse);
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in prepareLocation, msg = " + ex.getMessage());
        }

        // neu chua co record hoac record khong đúng LogID
        if(exist){
            // tinh distance va bearing
            tracker.setDistance(util.distance(tracker.getPrevLat(), tracker.getPrevLng(), tracker.getLat(), tracker.getLng()));
//            tracker.setAngle(util.bearing(tracker.getPrevLat(), tracker.getPrevLng(), tracker.getLat(), tracker.getLng()));
        }else {
            tracker.setLogID(0);
//            tracker.setAngle(0);
//            tracker.setDistance(0);
        }
        // pre processing input value
        if(tracker.isFuelEnable()){
            int prevFuelValue = 0;
            long prevFuelTime = 0;
            try{
                con = util.getRtsConnection();
                st = con.createStatement();
                sql = "SELECT current_value, log_time from fuel WHERE fuel_id=" + tracker.getFuelId();
                rs = st.executeQuery(sql);

                if(rs.next()){
                    prevFuelValue = rs.getInt("current_value");
                    prevFuelTime = rs.getLong("log_time");
                }else{
                    // 21/11/22 : neu chua co record trong device_log thi prevFuel=fuel
                    prevFuelValue = tracker.getFuel();
                }
                st.close();
                con.close();
            }catch(Exception ex) {
                ex.printStackTrace();
                this.writeAdapterLog("Exception in prepareLocation, isFuelEnable(), msg = " + ex.getMessage());
            }finally{
                try{
                    st.close();
                    con.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
            tracker.setPrevFuel(prevFuelValue);
            tracker.setPrevFuelTime(prevFuelTime);
        }

        if(tracker.isTempEnable() && tracker.getTemp()>tracker.getTempLimit()){
            // canh bao temperature
//            tracker.setOverTemp(true);
        }

        if(tracker.isAirconEnable()){
            // xử lý lưu trạng thái aircon giống như power
        }

        if(tracker.isTruckEnable()){
            // xử lý lưu trạng thái truck giống như power
        }

        if(tracker.isDvrEnable()){
            // DVR chưa xử lý
        }

        // lay GEO
//        Connection geoCon = null;
//        CallableStatement cs = null;
//        String address = "";
//        try {
//            geoCon = util.getGcConnection();
//            cs = geoCon.prepareCall("{?=call getGoogleGeo(?,?)}");
//            cs.setFloat(2, tracker.getLat());// lat
//            cs.setFloat(3, tracker.getLng());// lng
//            cs.registerOutParameter(1, Types.NVARCHAR);// address
//            cs.execute();
//            address = cs.getString(1);
//            cs.close();
//            geoCon.close();
//        }catch(Exception ex) {
//            ex.printStackTrace();
//            this.writeAdapterLog("Exception in prepareLocation, getAddress, msg = " + ex.getMessage());
//        }
        tracker.setAddress(getMyGeo(tracker.getLat(), tracker.getLng()));
    }

    // xu ly sau khi prepareLocation
    public void updateLocation(Tracker tracker) {
        try {
            if(tracker.getLogID()>0) {
                // neu da co record : xet 2 truong hop : speed>=MIN_MOVING_SPEED va speed<MIN_MOVING_SPEED
                if(tracker.getSpeed() >= Tracker.MIN_MOVING_SPEED) {
                    // moving : xet 2 truong hop : truoc do moving hoac truoc do stop
                    // ca 2 case deu can phai update time record cu + tao moi record, chi khac nhau DRIVING
                    if(tracker.getPrevSpeed() >= Tracker.MIN_MOVING_SPEED) {
                        // truoc do MOVING : MOVING -> MOVING
                        processMovingToMovingLog(tracker);
//                        System.out.println("moving -> moving");
                    }else{
                        // truoc do STOP : STOP -> MOVING
                        processStopToMovingLog(tracker);
//                        System.out.println("stop -> moving");
                    }
                }else{
                    // stop : chia 2 case : neu truoc do stop hoac dang moving
                    // ca 2 case deu can phai update time record cu, khong update status
                    if(tracker.getPrevSpeed() >= Tracker.MIN_MOVING_SPEED) {
                        // MOVING -> STOP : truoc do moving, update time, create new record
                        processMovingToStopLog(tracker);
//                        System.out.println("moving -> stop");
                    }else{
                        // STOP -> STOP : hiện tại STOP, trước đó cũng STOP : update time + status + latlng + các input event
                        processStopToStopLog(tracker);
//                        System.out.println("stop -> stop");
                    }
                }
                // xử lý các bảng input : power, fuel, temp, aircon, truck, door
                if((tracker.getElapse() > 0) && tracker.isLegalTime()) processPower(tracker);// Quang edit 02/01/23 : chi xu ly khi isLegalTime() va elapse>0
                if((tracker.getElapse() > 0) && tracker.isLegalTime() && tracker.isFuelEnable()) processFuel(tracker);// Quang add 22/02/23
            }else {
                // nếu chưa có record
                processCreateRecordLog(tracker);
            }
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in updateLocation, msg = " + ex.getMessage());
        }
    }

    // Quang add 30/07/22
    public void updateMileage(Tracker tracker) {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "", address = "";
        if(tracker.getAddress() == null) {
            address = "null";
        }else {
            address = "'" + tracker.getAddress() + "'";
        }
        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            sql = "INSERT INTO mileage(mileage_id, device_id, imei, latitude, longitude, " +
                    "receive_time, description, position) " +
                    "VALUES (null," + tracker.getID() + ",'" + tracker.getIMEI() + "'," + tracker.getLat() + "," +
//                    "" + tracker.getLng() + "," + tracker.getTime() + ",'',";
                    "" + tracker.getLng() + "," + tracker.getTime() + ",''," + address + ")";
//            if(tracker.getAddress()==null) {
//                sql += "null)";
//            }else {
//                sql += "'" + tracker.getAddress() + "')";
//            }
            st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            rs = st.getGeneratedKeys();
            if(rs!=null && rs.next()) {
                long mileageLastInsertID = rs.getLong(1);
                tracker.setMileageID(mileageLastInsertID);
            }
            st.close();
            con.close();
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in updateMileage, msg = " + ex.getMessage());
        }
    }

    // --------------------------------------------------
    // da co record : dang MOVING (truoc do STOP hoac MOVING deu phai update record cu va tao record moi)
    public void processMovingToMovingLog(Tracker tracker){
//        System.out.println("processLogMovingToMoving, tracker name = " + tracker.getName());
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "";
        String address = "";
        long logLastInsertID = 0;
        int rowCount = 0;
        long vioLastInsertID = 0;
        if(tracker.getAddress() == null) {
            address = "null";
        }else {
            address = "'" + tracker.getAddress() + "'";
        }
        try{
            // Quang edit 18/10/22 : khoi phuc lai day_stretch : update vao cot trong device_log
            tracker.setDayStretch(tracker.getDayStretch() + tracker.getDistance());
            int movingElapse = tracker.getPower()? tracker.getElapse():0;
//            tracker.setMovingElapse(tracker.getMovingElapse() + movingElapse);
            tracker.setDayMovingElapse(tracker.getDayMovingElapse() + movingElapse);

            con = util.getRtsConnection();
            st = con.createStatement();
            sql = "INSERT INTO device_log(device_id, latitude, longitude, angle," +
                    "speed, gps, power, fuel_raw, fuel_value, temp_value, over_temp, " +
                    "aircon_value, truck_value, door, di2, begin_time, end_time, " +
                    "elapse, stretch, over_speed, moving_elapse, driver_name, " +
                    "driver_license, day_stretch, driving_elapse, is_overtime, position) " +
                    "VALUES (" + tracker.getID() + "," + tracker.getLat() + "," +
                    "" + tracker.getLng() + "," + tracker.getAngle() + "," + tracker.getSpeed() + "," +
                    "" + tracker.isGps() + "," + tracker.getPower() + "," +
                    "" + tracker.getFuelRaw() + "," + tracker.getFuel() + "," +
                    "" + tracker.getTemp() + "," + tracker.isOverTemp() + "," +
                    "" + tracker.getAircon() + "," + tracker.getTruck() + "," +
//                    "" + tracker.getSD() + "," + tracker.getDi2() + ", unix_timestamp(now())," +
                    "" + tracker.getSD() + "," + tracker.getDi2() + ", " + tracker.getTime() + "," + tracker.getTime() +
                    "," + tracker.getElapse() + "," + tracker.getDistance() + "," + tracker.isOverSpeed() + ",0,'" +
                    "" + tracker.getDriverName() + "','" + tracker.getDriverLicense() + "'," + tracker.getDayStretch() + "," +
                    "" + tracker.getDrivingElapse() + "," + tracker.isDrivingOvertime() + "," + address + ")";
//            if(tracker.getAddress()==null) {
//                sql += "null)";
//            }else {
//                sql += "'" + tracker.getAddress() + "')";
//            }
            st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            rs = st.getGeneratedKeys();
            if(rs!=null && rs.next()) {
                logLastInsertID = rs.getLong(1);
                sql = "UPDATE device SET log_id=" + logLastInsertID + ", day_stretch=" + tracker.getDayStretch() +
                        ", day_elapse=" + tracker.getDayMovingElapse() + " WHERE device_id=" + tracker.getID();
                rowCount = st.executeUpdate(sql);
                tracker.setLogID(logLastInsertID);// update logID moi nhat
            }

            //----------------------------------------------------------------------------------------------------
            // xu ly overspeed violation theo TT09
            if(tracker.getSpeed() >= (tracker.getSpeedLimit() + 5)){
                // gia su rang 10<elapse<60
                // avgSpeed = distance/elapse, luc nay da co distance
                // gia lap distance=500m
                int avgSpeed= (int)((tracker.getDistance()/tracker.getElapse()) * 3.6);// quy doi tu m/s -> km/h
//                int avgSpeed= (int)((500/tracker.getElapse()) * 3.6);// quy doi tu m/s -> km/h
                int deltaSpeed = avgSpeed - 5;
                // neu ssSpeed > speedLimit : bat dau mark cho begin

                if(deltaSpeed > tracker.getSpeedLimit()){
                    // vi pham : co 2 truong hop : chua vi pham hoac dang vi pham
                    // luc dau tien violation -> luu startTime
                    int vioSpeed = deltaSpeed - tracker.getSpeedLimit();
                    String coor = tracker.getLng() + "," + tracker.getLat();
//                    System.out.println("elapse="+tracker.getElapse()+",avgSpeed="+avgSpeed+", deltaSpeed="+deltaSpeed+",speedVioID="+ tracker.getSpeedVioID());
                    if(tracker.getSpeedVioID() > 0){
                        // dang violation : lay cac gia tri hien tai, SUM hoac MAX
                        int stret = 0;
                        int elap = 0;
                        int vio_s = 0;
                        sql = "SELECT stretch, elapse, vio_speed FROM speed_violation WHERE vio_id=" + tracker.getSpeedVioID();
                        con = util.getRtsConnection();
                        st = con.createStatement();
                        rs = st.executeQuery(sql);

                        if(rs.next()) {
                            stret = rs.getInt("stretch");
                            elap = rs.getInt("elapse");
                            vio_s = rs.getInt("vio_speed");
                        }
                        stret += tracker.getDistance();
//                        stret += 500;
                        elap += tracker.getElapse();
                        if(vio_s <= vioSpeed){
                            // lay MAX
                            vio_s = vioSpeed;
                        }
                        sql = "UPDATE speed_violation SET end_time=" + tracker.getTime() + ", end_coordinate='" + coor +
                                "', end_position=" + address + ", stretch=" +
                                stret + ", elapse=" + elap + ", vio_speed=" + vio_s +
                                " WHERE vio_id=" + tracker.getSpeedVioID();
                        st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                    }else{
                        // chua, bat dau violation
                        sql = "INSERT INTO speed_violation(vio_id, device_id, start_time, start_coordinate, " +
                                "start_position, end_time, end_coordinate, end_position, stretch, elapse, vio_speed) " +
                                "VALUES (null," + tracker.getID() + ","+tracker.getTime() + ",'" + coor + "'," + address + "," +
                                "" + tracker.getTime() + ",'" + coor + "'," + address + "," + tracker.getDistance() + "," + tracker.getElapse() + "," + (deltaSpeed-tracker.getSpeedLimit()) + ")";
//                                "" + tracker.getTime() + ",'" + coor + "'," + address + "," + 500 + "," + tracker.getElapse() + "," + (deltaSpeed-tracker.getSpeedLimit()) + ")";
//                        System.out.println(sql);
                        st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                        rs = st.getGeneratedKeys();
                        if(rs!=null && rs.next()) {
                            vioLastInsertID = rs.getLong(1);
                            // update vioLastInserID vao bang device
                            sql = "UPDATE device SET speed_vio_id=" + vioLastInsertID +
                                    " WHERE device_id=" + tracker.getID();
                            rowCount = st.executeUpdate(sql);
                            tracker.setSpeedVioID(vioLastInsertID);// update speedVioID moi nhat
                        }
                    }
                    // luu vao bang speed_violation_detail : luc nay luon luon co speedVioID
                    sql = "INSERT INTO speed_violation_detail(vio_detail_id, vio_id, device_id, time, coordinate, avg_speed, elapse, stretch, position) " +
                            "VALUES (null," + tracker.getSpeedVioID() + "," + tracker.getID() + ","+tracker.getTime() + ",'" + coor + "'," + avgSpeed + "," +
                            tracker.getElapse() + "," +
                            tracker.getDistance() + "," + address + ")";
//                            "" + 500 + "," + address + ")";
                    st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                }else{

                }
//                System.out.println("violation, elapse=" + tracker.getElapse() +",speed="+ tracker.getSpeed()+",is op="+tracker.isOverSpeed()+",limit="+tracker.getSpeedLimit());
            }else{
                // khong violation
                if(tracker.getSpeedVioID() > 0){
                    // neu dang violation thi update bang device
                    sql = "UPDATE device SET speed_vio_id=0" +
                            " WHERE device_id=" + tracker.getID();
                    st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                    tracker.setSpeedVioID(0);
                }
            }
            st.close();
            con.close();
        }catch(Exception ex){
            ex.printStackTrace();
            this.writeAdapterLog("Exception in processMovingToMovingLog, msg = " + ex.getMessage());
        }
    }

    public void processStopToMovingLog(Tracker tracker){
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "", address = "";
        long logLastInsertID = 0;
        int rowCount = 0;
        if(tracker.getAddress() == null) {
            address = "null";
        }else {
            address = "'" + tracker.getAddress() + "'";
        }
        try{
            // Quang edit 18/10/22 : khoi phuc lai day_stretch : update vao cot trong device_log
            tracker.setDayStretch(tracker.getDayStretch() + tracker.getDistance());

            con = util.getRtsConnection();
            st = con.createStatement();
            sql = "UPDATE device_log SET gps=" + tracker.isGps() + ", " +
                    "end_time=" + tracker.getTime() + ", elapse=" + tracker.getDuration() + " WHERE log_id=" + tracker.getLogID();
            rowCount = st.executeUpdate(sql);
            // luc nay dung elapse la thoi gian giua 2 msg
            sql = "INSERT INTO device_log(device_id, latitude, longitude, angle," +
                    "speed, gps, power, fuel_raw, fuel_value, temp_value, over_temp, " +
                    "aircon_value, truck_value, door, di2, begin_time, end_time, " +
                    "elapse, stretch, over_speed, moving_elapse, driver_name, " +
                    "driver_license, day_stretch, driving_elapse, is_overtime, position) " +
                    "VALUES (" + tracker.getID() + "," + tracker.getLat() + "," +
                    "" + tracker.getLng() + "," + tracker.getAngle() + "," + tracker.getSpeed() + "," +
                    "" + tracker.isGps() + "," + tracker.getPower() + "," +
                    "" + tracker.getFuelRaw() + "," + tracker.getFuel() + "," +
                    "" + tracker.getTemp() + "," + tracker.isOverTemp() + "," +
                    "" + tracker.getAircon() + "," + tracker.getTruck() + "," +
                    "" + tracker.getSD() + "," + tracker.getDi2() + "," + tracker.getTime() + "," + tracker.getTime() +
                    "," + tracker.getElapse() + "," + tracker.getDistance() + "," + tracker.isOverSpeed() + ",0,'" +
                    "" + tracker.getDriverName() + "','" + tracker.getDriverLicense() + "'," + tracker.getDayStretch() + "," +
                    "" + tracker.getDrivingElapse() + "," + tracker.isDrivingOvertime() + "," + address + ")";
//            if(tracker.getAddress()==null) {
//                sql+= "null)";
//            }else {
//                sql+= "'" + tracker.getAddress() + "')";
//            }
            st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            rs = st.getGeneratedKeys();
            if(rs!=null && rs.next()) {
                logLastInsertID = rs.getLong(1);
                // Quang edit 16/10/22 : khoi phuc lai day_stretch
                // update lastInserID vao bang device
                sql = "UPDATE device SET log_id=" + logLastInsertID + ", day_stretch=" + tracker.getDayStretch() +
                        " WHERE device_id=" + tracker.getID();
                rowCount = st.executeUpdate(sql);
                tracker.setLogID(logLastInsertID);// update logID moi nhat
            }
            // kiem tra speed violation theo TT09
            if(tracker.getSpeedVioID() > 0){
                // neu dang violation thi update bang device
                sql = "UPDATE device SET speed_vio_id=0" +
                        " WHERE device_id=" + tracker.getID();
                st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                tracker.setSpeedVioID(0);
            }
            st.close();
            con.close();
        }catch(Exception ex){
            ex.printStackTrace();
            this.writeAdapterLog("Exception in processStopToMovingLog, msg = " + ex.getMessage());
        }
    }

    // đã có record, đang STOP mà trước đó cũng STOP -> edit
    public void processStopToStopLog(Tracker tracker){
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "";
        int rowCount = 0;

        try{
            con = util.getRtsConnection();
            st = con.createStatement();
            sql = "UPDATE device_log SET latitude=" + tracker.getLat() + ", " +
                    "longitude=" + tracker.getLng() + ", speed=" + tracker.getSpeed() + ", " +
                    "gps=" + tracker.isGps() +", power="+ tracker.getPower()+", " +
                    "fuel_raw="+tracker.getFuelRaw() + ", fuel_value=" + tracker.getFuel() + ", " +
                    "temp_value=" + tracker.getTemp() + ", over_temp=" + tracker.isOverTemp() + ", " +
                    "aircon_value=" + tracker.getAircon() + ", truck_value=" +
                    "" + tracker.getTruck() + ", door=" + tracker.getSD() + ", " +
                    "di2=" + tracker.getDi2() + ", " +
                    "end_time=" + tracker.getTime() +
                    ", elapse=" + tracker.getDuration() + ", driver_name='" + tracker.getDriverName() + "', " +
                    "driver_license='" + tracker.getDriverLicense() + "', driving_elapse=" +
                    "" + tracker.getDrivingElapse() + ", is_overtime=" + tracker.isDrivingOvertime() +
                    " WHERE log_id=" + tracker.getLogID();
            rowCount = st.executeUpdate(sql);
            // kiem tra speed violation theo TT09
            if(tracker.getSpeedVioID() > 0){
                // neu dang violation thi update bang device
                sql = "UPDATE device SET speed_vio_id=0" +
                        " WHERE device_id=" + tracker.getID();
                st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                tracker.setSpeedVioID(0);
            }
            st.close();
            con.close();
//            System.out.println("inside StopToStop," + "driver="+tracker.getDriverName()+",license="+tracker.getDriverLicense());
        }catch(Exception ex){
            ex.printStackTrace();
            this.writeAdapterLog("Exception in processStopToStopLog, msg = " + ex.getMessage());
        }
    }

    // da co record, dang STOP ma truoc do moving -> edit
    public void processMovingToStopLog(Tracker tracker){
//        System.out.println("processLogMovingToStop, tracker name = " + tracker.getName());
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "", address = "";
        long lastInsertID = 0;
        int rowCount = 0;
        if(tracker.getAddress() == null) {
            address = "null";
        }else {
            address = "'" + tracker.getAddress() + "'";
        }
        try{
            // Quang edit 18/10/22 : khoi phuc lai day_stretch : update vao cot trong device_log
            tracker.setDayStretch(tracker.getDayStretch() + tracker.getDistance());
//            int movingElapse = tracker.getPower()? tracker.getElapse():0;
//            tracker.setMovingElapse(tracker.getMovingElapse() + movingElapse);
//            tracker.setDayMovingElapse(tracker.getDayMovingElapse() + tracker.getMovingElapse());

            con = util.getRtsConnection();
            st = con.createStatement();
            sql = "INSERT INTO device_log(device_id, latitude, longitude, angle," +
                    "speed, gps, power, fuel_raw, fuel_value, temp_value, over_temp, " +
                    "aircon_value, truck_value, door, di2, begin_time, end_time, " +
                    "elapse, stretch, over_speed, moving_elapse, driver_name, " +
                    "driver_license, day_stretch, driving_elapse, is_overtime, position) " +
                    "VALUES (" + tracker.getID() + "," + tracker.getLat() + "," +
                    "" + tracker.getLng() + "," + tracker.getAngle() + "," + tracker.getSpeed() + "," +
                    "" + tracker.isGps() + "," + tracker.getPower() + "," +
                    "" + tracker.getFuelRaw() + "," + tracker.getFuel() + "," +
                    "" + tracker.getTemp() + "," + tracker.isOverTemp() + "," +
                    "" + tracker.getAircon() + "," + tracker.getTruck() + "," +
                    "" + tracker.getSD() + "," + tracker.getDi2() + "," + tracker.getTime() + "," + tracker.getTime() +
                    "," + tracker.getElapse() + "," + tracker.getDistance() + "," + tracker.isOverSpeed() + ",0,'" +
                    "" + tracker.getDriverName() + "','" + tracker.getDriverLicense() + "'," + tracker.getDayStretch() + "," +
                    "" + tracker.getDrivingElapse() + "," + tracker.isDrivingOvertime() + "," + address + ")";
//            if(tracker.getAddress()==null) {
//                sql+= "null)";
//            }else {
//                sql+= "'" + tracker.getAddress() + "')";
//            }
            st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            rs = st.getGeneratedKeys();
            if(rs!=null && rs.next()) {
                lastInsertID = rs.getLong(1);
                // update lastInserID vao bang device
                sql = "UPDATE device SET log_id=" + lastInsertID + ", day_stretch=" + tracker.getDayStretch() +
                        " WHERE device_id=" + tracker.getID();
                rowCount = st.executeUpdate(sql);
                tracker.setLogID(lastInsertID);// update logID moi nhat
            }
            //----------------------------------------------------------------------------------------------------
            // kiem tra speed violation theo TT09
            if(tracker.getSpeedVioID() > 0){
                // neu dang violation thi update bang device
                sql = "UPDATE device SET speed_vio_id=0 WHERE device_id=" + tracker.getID();
                st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                tracker.setSpeedVioID(0);
            }
            st.close();
            con.close();
        }catch(Exception ex){
            ex.printStackTrace();
            this.writeAdapterLog("Exception in processMovingToStopLog, msg = " + ex.getMessage());
        }
    }

    // truong hop chua co record -> create
    public void processCreateRecordLog(Tracker tracker){
//        System.out.println("processLogCreateRecord, tracker name = " + tracker.getName());
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "", address = "";
        long lastInsertID = 0;
        int rowCount = 0;
        if(tracker.getAddress() == null) {
            address = "null";
        }else {
            address = "'" + tracker.getAddress() + "'";
        }
        sql = "INSERT INTO device_log(device_id, latitude, longitude, angle," +
                "speed, gps, power, fuel_raw, fuel_value, temp_value, over_temp, " +
                "aircon_value, truck_value, door, di2, begin_time, end_time, " +
                "elapse, stretch, over_speed, moving_elapse, driver_name, " +
                "driver_license, day_stretch, driving_elapse, is_overtime, position) " +
                "VALUES (" + tracker.getID() + "," + tracker.getLat() + "," +
                "" + tracker.getLng() + "," + tracker.getAngle() + "," + tracker.getSpeed() + "," +
                "" + tracker.isGps() + "," + tracker.getPower() + "," +
                "" + tracker.getFuelRaw() + "," + tracker.getFuel() + "," +
                "" + tracker.getTemp() + "," + tracker.isOverTemp() + "," +
                "" + tracker.getAircon() + "," + tracker.getTruck() + "," +
                "" + tracker.getSD() + "," + tracker.getDi2() + "," + tracker.getTime() + "," + tracker.getTime() +
                ",0,0," + tracker.isOverSpeed() + ",0,'" +
                "" + tracker.getDriverName() + "','" + tracker.getDriverLicense() + "',0," +
                "" + tracker.getDrivingElapse() + "," + tracker.isDrivingOvertime() + "," + address + ")";
//        if(tracker.getAddress()==null) {
//            sql+= "null)";
//        }else {
//            sql+= "'" + tracker.getAddress() + "')";
//        }
        try{
//            System.out.println(tracker.getLat()+"--"+tracker.getLng());
            con = util.getRtsConnection();
            st = con.createStatement();
            st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            rs = st.getGeneratedKeys();
            if(rs!=null && rs.next()) {
                lastInsertID = rs.getLong(1);
                sql = "UPDATE device"
                        + " SET log_id=" + lastInsertID + ", day_stretch=" + tracker.getDayStretch()
                        + " WHERE device_id=" + tracker.getID();
                rowCount = st.executeUpdate(sql);
                tracker.setLogID(lastInsertID);// update logID moi nhat
            }
            st.close();
            con.close();
        }catch(Exception ex){
            ex.printStackTrace();
            this.writeAdapterLog("Exception in processCreateRecordLog, msg = " + ex.getMessage());
        }
    }
//----------------------------------------------------------------------------------------------------
    // thao tác bang power : Quang comment 02/06/22
    private void processPower(Tracker tracker){
        // logID moi nhat la tracker.getLogID()
//        System.out.println("processPower, tracker name = " + tracker.getName());
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "";
        long lastInsertID = 0;
        int rowCount = 0;
        long endLogTime = 0;
        int powerElapse = 0;
        boolean powerAction = false;
        int newMovingElapse = 0;
        // neu ACC va speed>0 thi movingElapse=tracker.getElapse()
        newMovingElapse = (tracker.getPower() && tracker.getSpeed()>0)? tracker.getElapse():0;

        sql = "SELECT power_id, end_log_time, moving_elapse, unix_timestamp(now())-begin_log_time as power_elapse, " +
                "action from power WHERE power_id=" + tracker.getPowerId();
        try{
            con = util.getRtsConnection();
            st = con.createStatement();
            rs = st.executeQuery(sql);
            if(rs.next()){
                // nếu có thì update
                endLogTime = rs.getLong("end_log_time");
                int currentMovingElapse = rs.getInt("moving_elapse");// Quang add 30/12/22
                powerElapse = rs.getInt("power_elapse") + tracker.getElapse();// Quang edit 27/12/22
                powerAction = rs.getBoolean("action");

                if(MyUtil.isNewDay(tracker.getTime(), endLogTime)){
                    // tao record moi khi qua ngay khac
                    sql = "INSERT INTO power VALUES(null,"+ tracker.getID() + "," +
                            ""+ tracker.getLogID() + ", unix_timestamp(now())," + tracker.getLogID() + "," +
                            "unix_timestamp(now()),0,0," + tracker.getPower() + ")";
                    st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                    rs = st.getGeneratedKeys();
                    if(rs!=null && rs.next()) {
                        lastInsertID = rs.getLong(1);
                        sql = "UPDATE device SET power_id=" + lastInsertID + " " +
                                "WHERE device_id=" + tracker.getID();
                        rowCount = st.executeUpdate(sql);
                    }
                }else{
                    // Quang add 22/12/22 : update moving_elapse : khi moving->stop thi device_log.moving_elapse=0
                    if(tracker.getPower()){
                        // ACC : update movingElapse
                        sql = "UPDATE power SET end_log_id=" + tracker.getLogID() + ", " +
                                "end_log_time=unix_timestamp(now()), moving_elapse=" + (currentMovingElapse + newMovingElapse) + ", elapse=" + powerElapse +
                                " WHERE power_id=" + tracker.getPowerId();
                    }else{
                        // ACC=0 : khong update movingElapse
                        sql = "UPDATE power SET end_log_id=" + tracker.getLogID() + ", " +
                                "end_log_time=unix_timestamp(now()), elapse=" + powerElapse +
                                " WHERE power_id=" + tracker.getPowerId();
                    }

                    rowCount = st.executeUpdate(sql);
                    // neu power dao trang thai thi insert record
                    if(tracker.getPower() != powerAction){
                        sql = "INSERT INTO power VALUES(null,"+ tracker.getID() + "," +
                                ""+ tracker.getLogID() + ", unix_timestamp(now())," + tracker.getLogID() + "," +
                                "unix_timestamp(now()),0,0," + tracker.getPower() + ")";
                        st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                        rs = st.getGeneratedKeys();
                        if(rs!=null && rs.next()) {
                            lastInsertID = rs.getLong(1);
                            sql = "UPDATE device SET power_id=" + lastInsertID + " " +
                                    "WHERE device_id=" + tracker.getID();
                            rowCount = st.executeUpdate(sql);
                        }
                    }
                }

            }else{
                // nếu chưa có record -> insert
                sql = "INSERT INTO power VALUES(null,"+ tracker.getID() + "," +
                        ""+ tracker.getLogID() + ", unix_timestamp(now())," + tracker.getLogID() + "," +
                        "unix_timestamp(now()),0,0," + tracker.getPower() + ")";
                st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                rs = st.getGeneratedKeys();
                if(rs!=null && rs.next()) {
                    lastInsertID = rs.getLong(1);
                    sql = "UPDATE device SET power_id=" + lastInsertID + " " +
                            "WHERE device_id=" + tracker.getID();
                    rowCount = st.executeUpdate(sql);
                }
            }
            st.close();
            con.close();
//            System.out.println("processPower, movingElapse="+tracker.getMovingElapse()+",power="+tracker.getPower()+",elapse="+tracker.getElapse());
        }catch(Exception ex){
            ex.printStackTrace();
            this.writeAdapterLog("Exception in processPower, msg = " + ex.getMessage());
        }
    }

    // thao tác bảng fuel
    private void processFuel(Tracker tracker){
        // logID moi nhat la tracker.getLogID()
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "";
        long lastInsertID = 0;
        int rowCount = 0;
        int delta = 0, fuelValue = 0;
        boolean fuelTrigger = false;

        if(tracker.getFuel() > 0){
            // khong bi ERR
            delta = tracker.getFuel() - tracker.getPrevFuel();
            fuelValue = tracker.getFuel();
        }else{
            // bi ERR, fuel = -1
            delta = 0;
            fuelValue = tracker.getPrevFuel();
        }

//        if(tracker.getTime() > (tracker.getPrevFuelTime() + 100) && (tracker.getMinute()%2 == 0)){
        if(tracker.getPower()){
            // ACC=ON : trigger chu ky 3min=180s
            if(tracker.getTime() >= tracker.getPrevFuelTime() + 180) fuelTrigger = true;
        }else{
            // ACC=OFF : trigger chu ky 9min=540s
            // neu do xong, chuyen qua ACC -> thoi gian se tang len them 9+3=12 min
            if(tracker.getTime() >= tracker.getPrevFuelTime() + 540) fuelTrigger = true;
        }
//        if(tracker.getTime() >= tracker.getPrevFuelTime() + 600){
        if(fuelTrigger){
            // neu hien tai > qua khu 100s va minute la so chan (even)
            // 18/11/22 : neu >= 15min (900s)
//            System.out.println("device_id="+ tracker.getID()+",current min="+tracker.getMinute()+", prev min="+tracker.getPrevFuelMinute()+", prevFuelTime="+tracker.getPrevFuelTime()+", currentTime="+tracker.getTime());
//            System.out.println(tracker.getPrevFuel()+",current fuel="+tracker.getFuel()+",time="+tracker.getTime());
            // -> insert record
            sql = "INSERT INTO fuel VALUES(null," + tracker.getID() + "," +
                    "" + tracker.getPrevFuel() + "," + fuelValue + "," +
                    "" + delta + "," + tracker.getLogID() + "," + tracker.getTime() + ")";
            try{
                con = util.getRtsConnection();
                st = con.createStatement();
                st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                rs = st.getGeneratedKeys();
                if(rs!=null && rs.next()) {
                    lastInsertID = rs.getLong(1);
                }
                sql = "UPDATE device SET fuel_id=" + lastInsertID + " " +
                        "WHERE device_id=" + tracker.getID();
                rowCount = st.executeUpdate(sql);
                st.close();
                con.close();
            }catch(Exception ex){
                ex.printStackTrace();
                this.writeAdapterLog("Exception in processFuel, msg = " + ex.getMessage());
            }finally{
                try{
                    st.close();
                    con.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    //----------------------------------------------------------------------------------------------------
    // khi miss GPS
    public void updateTime(Tracker tracker) {
        // lay BeginTime cua prev record
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String sql = "";

        // pre processing input value
        if(tracker.isFuelEnable()){
            int prevFuelValue = 0;
            long prevFuelTime = 0;
            try{
                con = util.getRtsConnection();
                st = con.createStatement();
                sql = "SELECT current_value, log_time from fuel WHERE fuel_id=" + tracker.getFuelId();
                rs = st.executeQuery(sql);

                if(rs.next()){
                    prevFuelValue = rs.getInt("current_value");
                    prevFuelTime = rs.getLong("log_time");
                }else{
                    // 21/11/22 : neu chua co record trong device_log thi prevFuel=fuel
                    prevFuelValue = tracker.getFuel();
                }
                st.close();
                con.close();
            }catch(Exception ex) {
                ex.printStackTrace();
                this.writeAdapterLog("Exception in prepareLocation, isFuelEnable(), msg = " + ex.getMessage());
            }finally{
                try{
                    st.close();
                    con.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
            tracker.setPrevFuel(prevFuelValue);
            tracker.setPrevFuelTime(prevFuelTime);
        }

        // update time device_log
        sql = "SELECT " + tracker.getTime() + "-begin_time as elapse"
                + " FROM device_log"
                + " WHERE log_id=" + tracker.getLogID();
        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            rs = st.executeQuery(sql);
            int elapse =0;
            if(rs.next()) {
                // has record
                elapse = rs.getInt("elapse");
            }

            tracker.setElapse(elapse);
            sql = "UPDATE device_log"
//                    + " SET gps=" + tracker.isGps() + ", end_time=unix_timestamp(now()), elapse=" + tracker.getElapse()
                    + " SET gps=" + tracker.isGps() + ", power=" + tracker.getPower() + ", fuel_raw="
                    + tracker.getFuelRaw() + ", fuel_value=" + tracker.getFuel() + ", temp_value="
                    + tracker.getTemp() + ", over_temp=" + tracker.isOverTemp()
                    + ", aircon_value=" + tracker.getAircon() + ", truck_value=" + tracker.getTruck()
                    + ", door=" + tracker.getSD() + ", di2=" + tracker.getDi2()
                    + ", end_time=" + tracker.getTime() + ", elapse=" + tracker.getElapse()
                    // Quang edit 25/05/22
                    + ", over_speed=" + tracker.isOverSpeed()
                    + ", driver_name='" + tracker.getDriverName() + "', driver_license='" + tracker.getDriverLicense()
                    + "', driving_elapse=" + tracker.getDrivingElapse()
                    + " WHERE log_id=" + tracker.getLogID();
            st.executeUpdate(sql);
            st.close();
            con.close();
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in updateTime, msg = " + ex.getMessage());
        }

        // xử lý các bảng input : power, fuel
        if((tracker.getElapse() > 0) && tracker.isLegalTime()) processPower(tracker);// Quang add 02/02/23
        if((tracker.getElapse() > 0) && tracker.isLegalTime() && tracker.isFuelEnable()) processFuel(tracker);// Quang add 22/02/23
    }

    private synchronized void updateStatus(Tracker tracker) {
        String sql = "UPDATE device_log"
                + " SET end_time=" + tracker.getTime() + ","
                + " elapse=" + tracker.getElapse() + ", power=" + tracker.getPower()
                + " WHERE log_id=" + tracker.getLogID();
        Connection con = null;
        Statement st = null;
        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            int rowCount = st.executeUpdate(sql);
            st.close();
            con.close();
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in updateStatus, msg = " + ex.getMessage());
        }
    }
    //----------------------------------------------------------------------------------------------------
    //----------------------------------------------------------------------------------------------------
    // Quang add 03/05/22 : process event msg, edit 07/01/23 : neu type=version thi result=fw
    private synchronized void updateEvent(String imei, String type, String result, String param, int time) {
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;

        String sql = "SELECT device_id FROM device WHERE imei='" + imei + "' LIMIT 1";
        boolean exist = false;
        int deviceID = 0;
        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            rs = st.executeQuery(sql);

            if(rs.next()) {
                deviceID = rs.getInt("device_id");
                exist = true;
            }
            st.close();
            if(exist) {
                st = con.createStatement();
                sql = "INSERT INTO tracker_event (event_id, device_id, imei, command, " +
                        "result, param, receive_time) " +
                        "VALUES (null," + deviceID + ",'" + imei + "','" +
                        "" + type + "','" + result + "','" + param + "'," + time + ")";
                int lastId = st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);


                if(type.equals("version")){
                    // update xu ly version ; result=firmware version
                    String version = imei.substring(0, imei.indexOf("_")) + " v" + result;
                    sql = "UPDATE device SET tracker_type='" + version + "' " +
                            "WHERE device_id=" + deviceID;
                    int rowCount = st.executeUpdate(sql);
                }else if(type.equals("network")){
                    // update sim_balance trong bang device
                    sql = "UPDATE device SET sim_balance='" + param + "' " +
                            "WHERE device_id=" + deviceID;
                    int rowCount = st.executeUpdate(sql);
                }
                st.close();
            }
            con.close();
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in updateEvent, msg = " + ex.getMessage());
        }
    }

    // Quang add 05/03/23, private synchronized void updatePhoto3Path(long deviceID, String photo3Path){
//    private synchronized void updatePhotoPath(long deviceID, int channel, float lat, float lng, int speed, long unixTime, String photoPath, String address){
    private synchronized void updatePhotoPath(Tracker t, int channel){
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;
        String address = "";

//        String sql = "UPDATE device SET photo3_path='" + photoPath + "' WHERE device_id=" + deviceID;
        if(t.getAddress() == null) {
            address = "null";
        }else {
            address = "'" + t.getAddress() + "'";
        }
        String sql = "INSERT INTO photo values(null," + t.getID() +"," + channel + "," + t.getLat() + "," +
                t.getLng() + "," + t.getSpeed() + "," + t.getTime() + ",'" + t.getPhoto3Path() + "',true," + address + ")";
        try{
            con = util.getRtsConnection();
            st = con.createStatement();
            st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            rs = st.getGeneratedKeys();
            if(rs!=null && rs.next() && (channel == 3)) {
                long photoLastInsertID = rs.getLong(1);
                sql = "UPDATE device SET photo3_id=" + photoLastInsertID + " WHERE device_id=" + t.getID();
                st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            }
//            System.out.println(sql+","+channel);
            con.close();
        }catch(Exception ex){
            ex.printStackTrace();
            this.writeAdapterLog("Exception in updatePhoto3Path, msg = " + ex.getMessage());
        }
    }

    // Quang add 03/05/22 : process QCVN L05 msg
    private synchronized void updateQCVNSpeedPerSec(String imei, int time, String data) {
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;
        String sql = "SELECT device_id FROM device WHERE imei='" + imei + "' LIMIT 1";
        boolean exist = false;
        int deviceID = 0;
        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            rs = st.executeQuery(sql);

            if(rs.next()) {
                deviceID = rs.getInt("device_id");
                exist = true;
            }
            st.close();
            if(exist) {
                st = con.createStatement();
                sql = "INSERT INTO speed (speed_id, device_id, imei, " +
                        "data, receive_time) " +
                        "VALUES (null," + deviceID + ",'" + imei +
                        "','" + data + "'," + time + ")";
                int lastId = st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                st.close();
            }
            con.close();
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in updateQCVNSpeedPerSec, msg = " + ex.getMessage());
        }
    }

    // Quang add 25/05/22 : process QCVN overspeed msg, không insert cột position
    private synchronized void updateQCVNOverSpeed(String imei, int time, String driverName, String driverLicense, int averageSpeed, int speedLimit, String coordinate, int stretch, String desc) {
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;
        String sql = "SELECT device_id FROM device WHERE imei='" + imei + "' LIMIT 1";
        boolean exist = false;
        int deviceID = 0;

        String[] arr = coordinate.split(",");
        float lat = Float.parseFloat(arr[0]);
        float lng = Float.parseFloat(arr[1]);
        // lay GEO
        String geoAddress = getMyGeo(lat, lng);
        String address = "";
        if(geoAddress == null) {
            address = "null";
        }else {
            address = "'" + geoAddress + "'";
        }

        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            rs = st.executeQuery(sql);

            if(rs.next()) {
                deviceID = rs.getInt("device_id");
                exist = true;
            }
            st.close();
            if(exist) {
                st = con.createStatement();
                sql = "INSERT INTO overspeed (overspeed_id, device_id, imei, " +
                        "driver_name, driver_license, average_speed, speed_limit, " +
                        "coordinate, receive_time, position, stretch, description) " +
                        "VALUES (null," + deviceID + ",'" + imei + "','" + driverName +
                        "','" + driverLicense + "'," + averageSpeed + "," + speedLimit +
//                        ",'" + coordinate + "'," + time + ",null," + stretch +",'" + desc + "')";
                        ",'" + coordinate + "'," + time + "," + address + "," + stretch +",'" + desc + "')";
                int lastId = st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                st.close();
            }
            con.close();
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in updateQCVNOverSpeed, msg = " + ex.getMessage());
        }
    }

    // Quang add 25/05/22 : process QCVN park msg, không insert cột position
    private synchronized void updateQCVNPark(String imei, int time, String driverName, String driverLicense, String coordinate, int elapse, String desc) {
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;
        String sql = "SELECT device_id FROM device WHERE imei='" + imei + "' LIMIT 1";
        boolean exist = false;
        int deviceID = 0;

        String[] arr = coordinate.split(",");
        float lat = Float.parseFloat(arr[1]);
        float lng = Float.parseFloat(arr[0]);
        // lay GEO
        String geoAddress = getMyGeo(lat, lng);
        String address = "";
        if(geoAddress == null) {
            address = "null";
        }else {
            address = "'" + geoAddress + "'";
        }

        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            rs = st.executeQuery(sql);

            if(rs.next()) {
                deviceID = rs.getInt("device_id");
                exist = true;
            }
            st.close();
            if(exist) {
                st = con.createStatement();
                sql = "INSERT INTO park (park_id, device_id, imei, " + "driver_name, driver_license, " +
                        "coordinate, elapse, receive_time, position, description) " +
                        "VALUES (null," + deviceID + ",'" + imei + "','" + driverName + "','" + driverLicense +
                        "','" + coordinate + "'," + elapse + "," + time + "," + address + ",'" + desc + "')";
                int lastId = st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                st.close();
            }
            con.close();
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in updateQCVNPark, msg = " + ex.getMessage());
        }
    }

    // Quang add 25/05/22 : process QCVN park msg, không insert cột position
    // deviceId, time, driverName, driverLicense, drivingElapse, drivingTimeLimit, isOvertime, startTime, startCoordinate, endTime, endCoordinate, data
    private synchronized void updateQCVNDriving(String imei, int time, String driverName, String driverLicense, int drivingElapse, int drivingStretch, int drivingTimeLimit, boolean isOvertime, int startTime, String startCoordinate, int endTime, String endCoordinate, String desc) {
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;
        String sql = "SELECT device_id FROM device WHERE imei='" + imei + "' LIMIT 1";
        boolean exist = false;
        int deviceID = 0;

        String[] arr = startCoordinate.split(",");
        float startLat = Float.parseFloat(arr[1]);
        float startLng = Float.parseFloat(arr[0]);
        arr = endCoordinate.split(",");
        float endLat = Float.parseFloat(arr[1]);
        float endLng = Float.parseFloat(arr[0]);
        // lay GEO
        String geoAddress = getMyGeo(startLat, startLng);
        String startAddress = "", endAddress = "";
        if(geoAddress == null) {
            startAddress = "null";
        }else {
            startAddress = "'" + geoAddress + "'";
        }
        geoAddress = getMyGeo(endLat, endLng);
        if(geoAddress == null) {
            endAddress = "null";
        }else {
            endAddress = "'" + geoAddress + "'";
        }

        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            rs = st.executeQuery(sql);

            if(rs.next()) {
                deviceID = rs.getInt("device_id");
                exist = true;
            }
            st.close();
            if(exist) {
                st = con.createStatement();
                sql = "INSERT INTO driving_op (driving_id, device_id, imei, name, license, " +
                        "elapse, stretch, time_limit, is_overtime, start_time, start_coordinate," +
                        "start_position, end_time, end_coordinate, end_position, receive_time, description) " +
                        "VALUES (null," + deviceID + ",'" + imei + "','" + driverName + "','" +
                        driverLicense + "'," + drivingElapse + "," + drivingStretch + "," + drivingTimeLimit + "," +
                        isOvertime + "," + startTime + ",'" + startCoordinate + "'," + startAddress + "," +
                        endTime + ",'" + endCoordinate + "'," + endAddress + "," + time + ",'" + desc + "')";
                int lastId = st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                st.close();
            }
            con.close();
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in updateQCVNDriving, msg = " + ex.getMessage());
        }
    }

    private boolean sendAMQP(String exchange, String routingKey, String providerTaxCode, String taxCode, String license, String numberPlate, long unixTime, float lat, float lng, int speed, int angle, boolean acc){
        UfmsProto.BaseMessage msg;
        byte[] data;
//        System.out.println("sendAMQP : channel=" + channel + ", isOpen=" + channel.isOpen()+", numberPlate="+numberPlate);
        if(channel == null || !channel.isOpen()) {
            System.out.println("channel is null");
            return false;
        }
        try{
            // build RegCompany message
//            UfmsProto.RegCompany.Builder com = UfmsProto.RegCompany.newBuilder();
//            com.setCompany(taxCode);// taxCode cua TTC
//            // send RegCompany message
//            UfmsProto.RegCompany regCom = com.build();
//            msg = UfmsProto.BaseMessage.newBuilder().setMsgType(UfmsProto.BaseMessage.MsgType.RegCompany).setExtension(UfmsProto.RegCompany.msg, regCom).build();
//            data = msg.toByteArray();
//            channel.basicPublish(exchange, routingKey, null, data);
//
//            // build RegDriver message
//            UfmsProto.RegDriver.Builder driver = UfmsProto.RegDriver.newBuilder();
//            driver.setDriver(license);// GPLX
//            // send RegDriver message
//            UfmsProto.RegDriver regDri = driver.build();
//            msg = UfmsProto.BaseMessage.newBuilder().setMsgType(UfmsProto.BaseMessage.MsgType.RegDriver).setExtension(UfmsProto.RegDriver.msg, regDri).build();
//            data = msg.toByteArray();
//            channel.basicPublish(exchange, routingKey, null, data);
//
//            // build RegVehicle message
//            UfmsProto.RegVehicle.Builder vehicle = UfmsProto.RegVehicle.newBuilder();
//            vehicle.setCompany(clientTaxCode);// taxCode cua khach hang?
//            vehicle.setDriver(license);// GPLX
//            vehicle.setVehicle(numberPlate);// BSX
//            vehicle.setVehicleType(UfmsProto.RegVehicle.VehicleType.Bus);// loai hinh van tai
////            vehicle.setVehicleType(UfmsProto.RegVehicle.VehicleType.Bus);//loai hinh van tai
//            // send RegVehicle message
//            UfmsProto.RegVehicle regVehi = vehicle.build();
//            msg = UfmsProto.BaseMessage.newBuilder().setMsgType(UfmsProto.BaseMessage.MsgType.RegVehicle).setExtension(UfmsProto.RegVehicle.msg, regVehi).build();
//            data = msg.toByteArray();
//            channel.basicPublish(exchange, routingKey, null, data);

            UfmsProto.WayPoint.Builder point = UfmsProto.WayPoint.newBuilder();
            //long epoch = (new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").parse("04/01/2015 1:30:00").getTime() / 1000);
//            int epoch = (int) ((new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime()) / 1000);
            point.setAircon(false);// dieu hoa
            point.setDatetime((int)unixTime);// unix time
            point.setDoor(false);// door
            point.setDriver(license);// GPLX
            point.setHeading(angle);//goc huong
            point.setIgnition(acc);// IOs
            point.setSpeed(speed);// km/h
            point.setVehicle(numberPlate);// BSX
            point.setX(lng);//wGS84
            point.setY(lat);//wGS84
            point.setZ(0);//do cao

            UfmsProto.WayPoint wp = point.build();
            msg = UfmsProto.BaseMessage.newBuilder().setMsgType(UfmsProto.BaseMessage.MsgType.WayPoint).setExtension(UfmsProto.WayPoint.msg, wp).build();
            data = msg.toByteArray();
            channel.basicPublish(exchange, routingKey, null, data);
        }catch(Exception ex){
            ex.printStackTrace();
//            System.out.println("co loi" + ex.getMessage());
            return false;
        }
        String param = "exchange=" + exchange + ", routingKey=" + routingKey + ", providerTaxCode=" + providerTaxCode + ", clientTaxCode=" + taxCode + ", license=" + license + ", numberPlate=" + numberPlate + ", unixTime=" + unixTime + ", lat=" + lat + ", lng=" + lng + ", speed=" + speed + ", angle=" + angle + ", acc=" + acc;
//        System.out.println("send AMQP : " + param);// Quang disable 29/12/22
        return true;
    }

    // update device_log sau khi send AMQP
    private synchronized void updateAMQPMileageLog(boolean success, int deviceID, long mileageID){
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;
        String sql = "UPDATE mileage SET amqp=" + success +
                " WHERE mileage_id=" + mileageID + " and device_id=" + deviceID;
        try {
            con = util.getRtsConnection();
            st = con.createStatement();
            st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            con.close();
        }catch(Exception ex) {
            ex.printStackTrace();
//            this.writeAdapterLog("Exception in updateAMQPLog, msg = " + ex.getMessage());
        }
    }

    private synchronized String getMyGeo(float lat, float lng){
        // lay GEO
        Connection geoCon = null;
        CallableStatement cs = null;
        String address = null;

//        System.out.println("getMyGeo:"+lat+","+lng);
        try {
            geoCon = util.getGcConnection();
            cs = geoCon.prepareCall("{?=call getGoogleGeo(?,?)}");
            cs.setFloat(2, lat);
            cs.setFloat(3, lng);
            cs.registerOutParameter(1, Types.NVARCHAR);// address
            cs.execute();
            address = cs.getString(1);
            cs.close();
            geoCon.close();
        }catch(Exception ex) {
            ex.printStackTrace();
            this.writeAdapterLog("Exception in getMyGeo, msg = " + ex.getMessage());
            address = null;
        }
//        System.out.println("my address="+address);
        return address;
    }
    //----------------------------------------------------------------------------------------------------
    private void writeAdapterLog(String msg){
        adapterLog.info(msg,"info");
//        System.out.println(msg);
    }

    private BufferedImage addTextToImage(byte[] imageData, String str1, String str2, String str3){
        BufferedImage img = null;
        try{
            ByteArrayInputStream bais = new ByteArrayInputStream(imageData);
            img = ImageIO.read(bais);
            int width = img.getWidth();
            int height = img.getHeight();
            Graphics g = img.getGraphics();
            g.setFont(new Font("Arial", Font.ROMAN_BASELINE, 16));
            g.setColor(new Color(255, 255, 255));
            AttributedString as1 = new AttributedString(str1);
            as1.addAttribute(TextAttribute.SIZE, 16);
            as1.addAttribute(TextAttribute.BACKGROUND, new Color(126, 126, 126, 150), 0, str1.length());
            AttributedString as2 = new AttributedString(str2);
            as2.addAttribute(TextAttribute.SIZE, 16);
            as2.addAttribute(TextAttribute.BACKGROUND, new Color(126, 126, 126, 150), 0, str2.length());
            AttributedString as3 = new AttributedString(str3);
            as3.addAttribute(TextAttribute.SIZE, 16);
            as3.addAttribute(TextAttribute.BACKGROUND, new Color(126, 126, 126, 150), 0, str3.length());
            g.drawString(as1.getIterator(), (int)((width*3)/100), (int)((height*5)/100));
            g.drawString(as2.getIterator(), (int)((width*80)/100), (int)((height*5)/100));
            g.drawString(as3.getIterator(), (int)((width*3)/100), (int)((height*95)/100));
            g.dispose();
            bais.close();
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return img;
    }
}