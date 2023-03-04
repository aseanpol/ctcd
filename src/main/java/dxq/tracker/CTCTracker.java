package dxq.tracker;

public class CTCTracker extends Tracker{
    public static String LOCATION = "location";
    public static String EVENT = "event";
    public static String COMMAND = "command";// server không dùng
    public static String QCVN = "qcvn";
    public static String IMAGE = "image";

    public CTCTracker(String type){
        super(type);
    }
}
