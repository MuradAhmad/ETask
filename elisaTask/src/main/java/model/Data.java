package model;

public class Data {
    private String node;
    private String alarmId;
    private String alarmEventTime;

    public Data(String node, String alarmId, String alarmTimestamp) {
        this.node = node;
        this.alarmId = alarmId;
        this.alarmEventTime = alarmTimestamp;
    }

    public String getNode() {
        return node;
    }

    public String getAlarmId() {
        return alarmId;
    }

    public String getAlarmTimestamp() {
        return alarmEventTime;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public void setAlarmId(String alarmId) {
        this.alarmId = alarmId;
    }

    public void setAlarmTimestamp(String alarmTimestamp) {
        this.alarmEventTime = alarmTimestamp;
    }
}
