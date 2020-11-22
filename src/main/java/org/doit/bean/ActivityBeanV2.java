package org.doit.bean;

public class ActivityBeanV2 {
    public String uid;
    public String aid;
    public String activityName;
    public String time;
    public int eventTime;
    public double longituge; //经度
    public double latitude; //纬度
    public String province;

    public ActivityBeanV2() {
    }

    public ActivityBeanV2(String uid, String aid, String activityName, String time, int eventTime,double longituge,double latitude, String province) {
        this.uid = uid;
        this.aid = aid;
        this.activityName = activityName;
        this.time = time;
        this.eventTime = eventTime;
        this.longituge=longituge;
        this.latitude=latitude;
        this.province = province;
    }

    @Override
    public String toString() {
        return "ActivityBeanV2{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", activityName='" + activityName + '\'' +
                ", time='" + time + '\'' +
                ", eventTime=" + eventTime +
                ", longituge=" + longituge +
                ", latitude=" + latitude +
                ", province='" + province + '\'' +
                '}';
    }

    public static ActivityBeanV2 of(String uid, String aid, String activityName, String time, int eventTime, double longituge, double latitude, String province) {
       return new ActivityBeanV2(uid, aid, activityName, time, eventTime, longituge, latitude,province);
    }
}
