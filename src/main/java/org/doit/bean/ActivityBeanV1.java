package org.doit.bean;

public class ActivityBeanV1 {
    public String uid;
    public String aid;
    public String activityName;
    public String time;
    public int eventType;
    public String province;
    public int count=1;
    public ActivityBeanV1() {
    }

    public ActivityBeanV1(String uid, String aid, String activityName, String time, int eventType, String province) {
        this.uid = uid;
        this.aid = aid;
        this.activityName = activityName;
        this.time = time;
        this.eventType = eventType;
        this.province = province;
    }

    @Override
    public String toString() {
        return "ActivityBean{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", activityName='" + activityName + '\'' +
                ", time='" + time + '\'' +
                ", eventType=" + eventType +
                ", count=" + count +
                ", province='" + province + '\'' +
                '}';
    }

    public static ActivityBeanV1 of(String uid, String aid, String activityName, String time, int eventType, String province) {
       return new ActivityBeanV1(uid, aid, activityName, time, eventType, province);
    }
}
