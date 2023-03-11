package dxq.tracker;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;
import java.util.TreeMap;

public class Tracker{
    public int UPLOAD_PACKAGE_TYPE = 0;
    public static final int UPLOAD_DATA_CODE = 1;
    public static final int UPLOAD_COMMAND_CODE = 2;
    public static final int UPLOAD_EVENT_CODE = 3;
    public static final int UPLOAD_ERROR_CODE = 0;
    public int DEVICE_STATUS_CASE;

    public static final int MIN_MOVING_SPEED = 5;
    public static final int CTCTRACKER = 1;
//    public static final int CONCOX = 2;
//    public static final int NASIA = 3;
    public static final int DRIVING_LEGAL_ELAPSE = 4*3600;

    protected long unixTime = 0;
    protected int deviceID = 0;
    protected String name = "";
    protected String imei = "";
    protected String type = "";
    protected float lat = 0;
    protected float lng = 0;
    protected float prevLat = 0;
    protected float prevLng = 0;
    protected int speed = 0;
    protected int prevSpeed = 0;
    protected int distance = 0;
    protected int angle = 0;
    protected boolean gps = true;
    protected boolean power = false;
    protected boolean prevPower = false;
    protected long prevBeginTime = 0;
    protected long prevEndTime = 0;
    protected int elapse = 0;
    protected boolean fixGPS = false;// Quang add 16/10/22
    //----------------------------------------------------------------------------------------------------
    protected String address = null;
    protected long logID = 0;
    //----------------------------------------------------------------------------------------------------
    // Q add 28/07/2021 : bổ sung field theo QCVN của DB rts
    protected boolean overSpeed = false;
    // Quang add 16/08/22
    protected boolean overSpeedViolation = false;
    protected String driverName = "";
    protected String driverLicense = "";
    // Quang add 30/05/22
    protected int drivingElapse = 0;
    protected boolean drivingOvertime = false;
    // Quang add 20/08/22
    protected long speedVioID = 0;
    //----------------------------------------------------------------------------------------------------
    // các peripherial : fuel, temp, aircon, truck, dvr của device_log
    protected int fuelRaw = 0;
    protected int prev_fuel_value = 0;
    protected long prevFuelTime = 0;
    protected int fuelValue = 0;
    protected int tempValue = 0;
    protected boolean over_temp = false;
    protected boolean aircon_value = false;
    protected boolean truck_value = false;
    // dvr chưa làm
    protected boolean sd = false;
    protected boolean di2 = false;
    // các field sau của bảng device, dùng để đối chiếu tinh toán
    protected long power_id = 0;
    protected boolean fuelEnable = false;
    protected int tank_capac = 0;
    protected int fuelDelta = 0;
    protected long fuelId = 0;
    protected boolean temp_enable = false;
    protected int temp_limit = 0;
    protected boolean aircon_enable = false;
    protected long aircon_id = 0;
    protected boolean truck_enable = false;
    protected long truck_id = 0;
    protected boolean dvr_enable = false;
    protected long dvr_id = -1;
    protected boolean cam1_enable = false;
    protected boolean cam2_enable = false;
    protected boolean cam3_enable = false;
    protected String photo1Path = "";
    protected String photo2Path = "";
    protected String photo3Path = "";
//    protected long door_id = -1;
    protected int day_stretch = 0;
//    protected int day_elapse = 0;
    protected boolean nd91 = true;
    protected int speed_limit = 70;
    protected long mileageID = 0;
//    protected long driving_id = -1;
    protected String taxCode = "";
    // Quang add 10/12/22
    protected TreeMap<Integer, Integer> calibMap = null;
    // Quang add 22/12/22
//    protected int movingElapse = 0;
    protected int dayMovingElapse = 0;
    protected String numberPlate = "";

    //----------------------------------------------------------------------------------------------------
    public Tracker(String type) {
        this.type = type;
    }
    //----------------------------------------------------------------------------------------------------
    public void setTime(long unixTime) {
        this.unixTime = unixTime;
    }
    public long getTime() {
        return this.unixTime;
    }
    public void setID(int deviceID) {
        this.deviceID = deviceID;
    }
    public int getID() {
        return this.deviceID;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getName() {
        return this.name;
    }
    public void setIMEI(String imei) {
        this.imei = imei;
    }
    public String getIMEI() {
        return this.imei;
    }
    public void setType(String type) {
        this.type = type;
    }
    public String getType() {
        return this.type;
    }
    public void setLat(float lat) {
        this.lat = lat;
    }
    public float getLat() {
        return this.lat;
    }
    public void setLng(float lng) {
        this.lng = lng;
    }
    public float getLng() {
        return this.lng;
    }
    public void setPrevLat(float prevLat) {
        this.prevLat = prevLat;
    }
    public float getPrevLat() {
        return this.prevLat;
    }
    public void setPrevLng(float prevLng) {
        this.prevLng = prevLng;
    }
    public float getPrevLng() {
        return this.prevLng;
    }
    public void setAngle(int angle) {
        this.angle = angle;
    }
    public int getAngle() {
        return this.angle;
    }
    public void setSpeed(int speed) {
        this.speed = speed;
    }
    public int getSpeed() {
        return this.speed;
    }
    public void setPrevSpeed(int prevSpeed) {
        this.prevSpeed = prevSpeed;
    }
    public int getPrevSpeed() {
        return this.prevSpeed;
    }

    public void setGps(boolean gps) {
        this.gps = gps;
    }
    public boolean isGps() {
        return this.gps;
    }
    public void setPower(boolean power) {
        this.power = power;
    }

    public boolean getPower() {
        return this.power;
    }

    public void setPrevPower(boolean prevPower) {
        this.prevPower = prevPower;
    }

    public boolean getPrevPower() {
        return this.prevPower;
    }

    public void setPrevBeginTime(long prevBeginTime) {
        this.prevBeginTime = prevBeginTime;
    }
    public long getDuration() {
        return this.unixTime - this.prevBeginTime;
    }

    public void setElapse(int elapse) {
        this.elapse = elapse;
    }
    public int getElapse() {
        return this.elapse;
    }
    // stretch=distance
    public void setDistance(int distance) {
        this.distance = distance;
    }
    public int getDistance() {
        return this.distance;
    }
    //----------------------------------------------------------------------------------------------------

    public void setAddress(String address) {
        if(address.equals("")) this.address= null;
        else{
            address = address.replace("'","");
            address = address.replace("\"","");
            this.address = address;
        }
    }
    public String getAddress() {
        return this.address;
    }
    public void setLogID(long logID) {
        this.logID = logID;
    }
    public long getLogID() {
        return this.logID;
    }
    //----------------------------------------------------------------------------------------------------

    // Q add 28/07 :
    public void setOverSpeed(boolean overSpeed){
        this.overSpeed = overSpeed;
    }

    public boolean isOverSpeed(){
        return this.overSpeed;
    }

    public void setDriverName(String driverName){
        this.driverName = driverName;
    }

    public String getDriverName(){
        return this.driverName;
    }

    public void setDriverLicense(String driverLicense){
        this.driverLicense = driverLicense;
    }

    public String getDriverLicense(){
        return this.driverLicense;
    }

    //----------------------------------------------------------------------------------------------------
    // Q add 28/07 : bổ sung fuel, . . .
    public void setFuelRaw(int fuelRaw){
        this.fuelRaw = fuelRaw;
    }

    public float getFuelRaw(){
        return this.fuelRaw;
    }

    public void setPrevFuel(int prevFuelValue){
        this.prev_fuel_value = prevFuelValue;
    }

    public int getPrevFuel(){
        return this.prev_fuel_value;
    }

    // Quang add 22/02/23
    public void setPrevFuelTime(long prevFuelTime){
        this.prevFuelTime = prevFuelTime;
    }

    public long getPrevFuelTime(){
        return this.prevFuelTime;
    }

    public int getFuel(){
        return this.fuelValue;
    }

    // Quang add 10/12/22
    public void setCalibMap(TreeMap<Integer, Integer> calibMap){
        this.calibMap = calibMap;
    }

    // Quang add 10/12/22
    public void calibFuel(int fuelRaw){
        int minLevel = 0;
        int maxLevel = 0;
        int minValue = 0;
        int maxValue = 0;

        Set<Integer> keySet = calibMap.keySet();
        int i = 0;
        Object[] keys = keySet.toArray();
        try{
            if(keys.length > 0){
                minLevel = (Integer)keys[0];
                maxLevel = (Integer)keys[0];
                minValue = calibMap.get(keys[0]);
                maxValue = calibMap.get(keys[0]);
                // cho nay xem lai khi no vuot qua index
//                System.out.println("calib length="+keys.length);
                for(i=0; i<keys.length-1; i++){
                    int keyItem = (Integer)keys[i];
//                    System.out.println("keyItem="+keyItem);
                    if((fuelRaw >= (Integer)keys[i]) && (fuelRaw <= (Integer)keys[i+1])){
                        minLevel = (Integer)keys[i];
                        maxLevel = (Integer)keys[i+1];
                    }
                }

                minValue = calibMap.get(minLevel);
                maxValue = calibMap.get(maxLevel);

                float sampleValue = (float)(maxValue - minValue)/(maxLevel - minLevel);
                int sampleNum = fuelRaw - minLevel;
                float sample = sampleNum * sampleValue;
                fuelValue = (int)(minValue + sample);
            }
        }catch(Exception ex){
            ex.printStackTrace();
            System.out.println("calib error");
        }
    }

    public void setTemp(int tempValue){
        this.tempValue = tempValue;
    }

    public int getTemp(){
        return this.tempValue;
    }

    public void setOverTemp(boolean overTemp){
        this.over_temp = overTemp;
    }

    public boolean isOverTemp(){
        return this.over_temp;
    }

    public void setAircon(boolean airconValue){
        this.aircon_value = airconValue;
    }

    public boolean getAircon(){
        return this.aircon_value;
    }

    public void setTruck(boolean truckValue){
        this.truck_value = truckValue;
    }

    public boolean getTruck(){
        return this.truck_value;
    }

    public void setSD(boolean sd){
        this.sd = sd;
    }

    public boolean getSD(){
        return this.sd;
    }

    public void setDi2(boolean di2){
        this.di2 = di2;
    }

    public boolean getDi2(){
        return this.di2;
    }
    //----------------------------------------------------------------------------------------------------
    // Q add 28/07 : các field sau thuộc bảng device
    public void setPowerId(long power_id){
        this.power_id = power_id;
    }

    public long getPowerId(){
        return this.power_id;
    }

    // Quang add 16/10/22
    public boolean isFixGPS() {
        return this.fixGPS;
    }
    public void setFixGPS(boolean fixGPS){
        this.fixGPS = fixGPS;
    }

    public void setFuelEnable(boolean fuel_enable){
        this.fuelEnable = fuel_enable;
    }

    public boolean isFuelEnable(){
        return this.fuelEnable;
    }

    public void setTankCapac(int tank_capac){
        this.tank_capac = tank_capac;
    }

    public int getTankCapac(){
        return this.tank_capac;
    }

    public void setFuelDelta(int fuel_delta){
        this.fuelDelta = fuel_delta;
    }

    public int getFuelDelta(){
        return this.fuelDelta;
    }

    public void setFuelId(long fuel_id){
        this.fuelId = fuel_id;
    }

    public long getFuelId(){
        return this.fuelId;
    }

    public void setTempEnable(boolean temp_enable){
        this.temp_enable = temp_enable;
    }

    public boolean isTempEnable(){
        return this.temp_enable;
    }

    public void setTempLimit(int temp_limit){
        this.temp_limit = temp_limit;
    }

    public int getTempLimit(){
        return this.temp_limit;
    }

    public void setAirconEnable(boolean aircon_enable){
        this.aircon_enable = aircon_enable;
    }

    public boolean isAirconEnable(){
        return this.aircon_enable;
    }

    public void setAirconId(long aircon_id){
        this.aircon_id = aircon_id;
    }

    public long getAirconId(){
        return this.aircon_id;
    }

    public void setTruckEnable(boolean truck_enable){
        this.truck_enable = truck_enable;
    }

    public boolean isTruckEnable(){
        return this.truck_enable;
    }

    public void setTruckId(long truck_id){
        this.truck_id = truck_id;
    }

    public long getTruckId(){
        return this.truck_id;
    }

    public void setDvrEnable(boolean dvr_enable){
        this.dvr_enable = dvr_enable;
    }

    public boolean isDvrEnable(){
        return this.dvr_enable;
    }

    public void setDvrId(long dvr_id){
        this.dvr_id = dvr_id;
    }

    public long getDvrId(){
        return this.dvr_id;
    }

    public void setCam1Enable(boolean cam1_enable){
        this.cam1_enable = cam1_enable;
    }

    public void setCam2Enable(boolean cam2_enable){
        this.cam2_enable = cam2_enable;
    }

    public void setCam3Enable(boolean cam3_enable){
        this.cam3_enable = cam3_enable;
    }

    public boolean isCam1Enable(){
        return this.cam1_enable;
    }

    public boolean isCam2Enable(){
        return this.cam2_enable;
    }

    public boolean isCam3Enable(){
        return this.cam3_enable;
    }

    public void setPhoto1Path(String photo1Path){
        this.photo1Path = photo1Path;
    }

    public void setPhoto2Path(String photo2Path){
        this.photo2Path = photo2Path;
    }

    public void setPhoto3Path(String photo3Path){
        this.photo3Path = photo3Path;
    }

    public String getPhoto1Path(){
        return this.photo1Path;
    }

    public String getPhoto2Path(){
        return this.photo2Path;
    }

    public String getPhoto3Path(){
        return this.photo3Path;
    }

    // Quang add 29/12/22
    public void setDayMovingElapse(int dayMovingElapse){
        this.dayMovingElapse = dayMovingElapse;
    }

    public int getDayMovingElapse(){
        return this.dayMovingElapse;
    }

    public void setDayStretch(int day_stretch){
        this.day_stretch = day_stretch;
    }

    public int getDayStretch(){
        return this.day_stretch;
    }

    public void setND91(boolean nd91) {
    this.nd91 = nd91;
}

    public boolean isND91() {
        return this.nd91;
    }

    public void setMileageID(long mileageID) {
        this.mileageID = mileageID;
    }

    public long getMileageID() {
        return this.mileageID;
    }

    public void setSpeedLimit(int speed_limit){
        this.speed_limit = speed_limit;
    }

    public int getSpeedLimit(){
        return this.speed_limit;
    }

    public void setDrivingElapse(int drivingElapse){
        this.drivingElapse = drivingElapse;
    }

    public int getDrivingElapse(){
        return this.drivingElapse;
    }

    public void setDrivingOvertime(boolean drivingOvertime){
        this.drivingOvertime = drivingOvertime;
    }

    public boolean isDrivingOvertime(){
        return this.drivingOvertime;
    }

    public void setSpeedVioID(long speedVioID){
        this.speedVioID = speedVioID;
    }

    public long getSpeedVioID(){
        return this.speedVioID;
    }
    //----------------------------------------------------------------------------------------------------
    // các method add sau 01/08/2021 ve trang thai packet
    // Quang add 20/09/22 : taxCode dung de amqp DRVN
    public void setTaxCode(String taxCode){
        this.taxCode = taxCode;
    }

    public String getTaxCode(){
        return this.taxCode;
    }

    // Quang add 04/01/23
    public boolean isLegalTime(){
        // ngay hien tai
        long currentUnixTime = Instant.now().getEpochSecond();
        long delta = Math.abs(currentUnixTime - unixTime);// lech thoi gian
        return (delta < 3*60)? true:false;
    }

    // Quang add 17/02/23
    public void setNumberPlate(String numberPlate){
        this.numberPlate = numberPlate;
    }

    public String getNumberPlate(){
        return this.numberPlate;
    }
}