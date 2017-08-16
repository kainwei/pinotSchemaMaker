import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by weizh on 2017/3/20.
 */
public class druidSchMaker {
//    String tableName = "platformusertags_user_coins_game_snapshoot";
//    String intervals = "2017-02-18/2017-02-20";
//    String metricsList = "last_week_liveness_days:count,last_2weeks_liveness_days:count,first_recharge_amount:count," +
//            "his_max_recharge_amount:count,30d_max_recharge_amount:count,30d_sum_recharge_days:count," +
//            "last_recharge_amount:count,7d_profit_game_days:count,his_contribution_degree:count,consume_level:count";
//    String dimension = "userid,retain_date,last_week_liveness,last_2weeks_liveness,first_recharge_date," +
//            "his_max_recharge_date,30d_max_recharge_date,30d_sum_recharge_amount,7d_profit_level_date," +
//            "last_recharge_date,7d_profit_amount,7d_profit_level,his_contribution_degree_date," +
//            "consume_level_date,consume_amount_7d";
//
//    String colums = "userid,retain_date,last_week_liveness,last_week_liveness_days,last_2weeks_liveness," +
//            "last_2weeks_liveness_days,first_recharge_amount,first_recharge_date,his_max_recharge_amount," +
//            "his_max_recharge_date,30d_max_recharge_amount,30d_max_recharge_date,30d_sum_recharge_amount," +
//            "30d_sum_recharge_days,last_recharge_date,last_recharge_amount,7d_profit_amount,7d_profit_level," +
//            "7d_profit_level_date,7d_profit_game_days,his_contribution_degree,his_contribution_degree_date," +
//            "consume_level,consume_level_date,consume_amount_7d";
//
//    String timeCols = "first_recharge_date";
//
//    String hdfsPath = "/home/weizh/pinot/data/platformusertags_user_coins_game_snapshoot";
    /*String tableName = "cdd_shuffle_userdeal";
    String intervals = "2016-03-31/2016-04-01";

    String  metricsList = "deal_date:doubleMax";
    String dimension = "id,pid,acc_num,acc_type,rel_pid,rel_acc_num,rel_acc_type,deal_class_id,deal_id,deal_type_id,deal_subtype_id,deal_place_id,debit_currency_id,debit_amount,debit_balance,credit_currency_id,credit_amount,credit_balance,outer_account_id,data_time,note";

    String colums = "id,pid,acc_num,acc_type,rel_pid,rel_acc_num,rel_acc_type,deal_date,deal_class_id,deal_id,deal_type_id,deal_subtype_id,deal_place_id,debit_currency_id,debit_amount,debit_balance,credit_currency_id,credit_amount,credit_balance,outer_account_id,data_time,note";

    String timeCols = "data_time";
    String hdfsPath = "/hive/warehouse/zhouxm.db/zhouxm_shuffle_userdeal/year=2016/month=03/day=31";*/
    /*String tableName = "lifecycle2_day_fact";
    String intervals = "2017-03-01/2017-03-02";
    String  metricsList = "pid:hyperUnique";
    String dimension = "c_time,register_week_id,register_type_id,channel_id,location_id,isp_id,os_id,terminer_id,score_level,user_level";
    String colums = "pid,date_time,c_time,register_week_id,register_type_id,channel_id,location_id,isp_id,os_id,terminer_id,score_level,user_level";
    String timeCols = "date_time";
    String hdfsPath = "/hive/warehouse/zhouxm.db/lifecycle2_day_fact/year=2017/month=03/day=01";
*/
    static Properties _properties;
    static String tableName = "majiang_winpercentage_userlist";
    static String timeCols = "cdate";
    static String intervals = "2017-03-01/2017-03-15";
    static String  metricsList = "match_time:doubleMin,profit:doubleSum";
    static String columns = "product_id,user_id,match_time,score,income,outcome,profit,cdate";
    static String dimension = "product_id,user_id,score,income,outcome,cdate";
    static String hdfsPath = "/hive/warehouse/yangyn.db/majiang_winpercentage_userlist/year=2017/month=03/";
    static String timeFormat = "YYYYMMdd";
    static String listDelimiter = ",";
    static String classLoader = "-javax.validation.,java.,javax.,org.apache.commons.logging.,org.apache.log4j.,org.apache.hadoop.";

    public static Map<String, Object> getNewMap(){
        return new HashMap<String, Object>();
    }

    public static void putMap(Map<String, Object> dstMap, String key, Object val){
        dstMap.put(key, val);
    }
    public static void addList(List<Object> dstList, Object element){
        dstList.add(element);
    }

    public static List<String> stringToList(String str){
        List<String> res = new ArrayList<String>();
        String [] arr = str.split(",");
        for(String s:arr){
            res.add(s);
        }
        return res;
    }

    public static List<Object> stringToMapList(String str){
        List<Object> res = new ArrayList<Object>();
        String [] arr = str.split(",");
        for(String s:arr){
            Map<String, String> tmp = new HashMap<String, String>();
            String [] subArr = s.split(":");
            tmp.put("fieldName", subArr[0]);
            tmp.put("name", subArr[0]);
            tmp.put("type", subArr[1]);
            res.add(tmp);
        }
        return res;
    }

    public Map<String, Object> dimensionsSepcMap() {
        Map<String, Object> res = getNewMap();
        res.put("dimensionExclusions", new ArrayList<Object>());
        res.put("dimensions",stringToList(dimension));
        res.put("spatialDimensions",new ArrayList<Object>());
        return res;
    }

    public Map<String, Object> timestampSpec() {
        Map<String, Object> res = getNewMap();
        //res.put("column", stringToList(timeCols));
        res.put("column", timeCols);
        res.put("format", timeFormat);
        return res;
    }

    public Map<String, Object> parseSpecMap() {
        Map<String, Object> res = getNewMap();
        res.put("columns", stringToList(dimension));
        res.put("dimensionsSpec", this.dimensionsSepcMap());
        res.put("format", "csv");
        res.put("timestampSpec", this.timestampSpec());
        res.put("columns", stringToList(columns));
        res.put("listDelimiter", listDelimiter);

        return res;
    }

    public Map<String, Object> parserMap() {
        Map<String, Object> res = getNewMap();
        res.put("parseSpec", this.parseSpecMap());
        res.put("type", "hadoopString");
        return res;

    }

    public Map<String, Object> ioConfigMap() {
        Map<String, Object> res = getNewMap();
        Map<String, Object> inputSpec = getNewMap();
        inputSpec.put("paths", hdfsPath);
        inputSpec.put("type", "static");
        res.put("inputSpec", inputSpec);
        res.put("type", "hadoop");
        return res;
    }
    public Map<String, Object> dataSchema(){
        Map<String, Object> res = getNewMap();
        res.put("dataSource", tableName);
        Map<String, Object> granularitySpecMap = getNewMap();
        granularitySpecMap.put("intervals", stringToList(intervals));
        granularitySpecMap.put("queryGranularity", "MINUTE");
        granularitySpecMap.put("segmentGranularity", "DAY");
        granularitySpecMap.put("type", "uniform");
        res.put("granularitySpec", granularitySpecMap);
        res.put("metricsSpec", stringToMapList(metricsList));
        res.put("parser", parserMap());
        return res;
    }

    public Map<String, Object> jobProperties(){
        Map<String, Object> res = getNewMap();
        res.put("mapreduce.job.classloader", "true");
        res.put("mapreduce.job.classloader.system.classes",
                classLoader);
        res.put("mapreduce.map.java.opts",
                "-server -Xmx1536m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps");
        res.put("mapreduce.reduce.java.opts",
                "-server -Xmx2560m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps");
        res.put("mapreduce.reduce.memory.mb", "8192");
        return res;
    }

    public Map<String, Object> partitionsSpec() {
        Map<String, Object> res = getNewMap();
        res.put("type", "index");
        res.put("targetPartitionSize", 5000000);
        res.put("maxPartitionSize", 7500000);
        return res;
    }

    public Map<String, Object> tuningConfig() {
        Map<String, Object> res = getNewMap();
        res.put("type", "hadoop");
        res.put("jobProperties",this.jobProperties());
        res.put("partitionsSpec", this.partitionsSpec());
        return res;
    }

    public Map<String, Object> spec(){
        Map<String, Object> res = getNewMap();
        res.put("dataSchema", this.dataSchema());
        res.put("ioConfig", this.ioConfigMap());
        res.put("tuningConfig", this.tuningConfig());
        return res;
    }

    public Map<String, Object> druidSchema(){
        Map<String, Object> res = getNewMap();
        res.put("spec", this.spec());
        res.put("type","index_hadoop");
        return res;
    }
    public static void main(String [] args ){
        Properties jobConf = null;
        jobConf = new Properties();
        try {
            if(args.length!=0)
            jobConf.load(new FileInputStream(args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }
        tableName = jobConf.getProperty("tableName", "majiang_winpercentage_userlist");
        timeCols = jobConf.getProperty("timeCols", "cdate");
        intervals = jobConf.getProperty("intervals","2017-03-01/2017-03-15");
        metricsList = jobConf.getProperty("metricsList", "match_time:doubleMin,profit:doubleSum");
        columns = jobConf.getProperty("columns", "product_id,user_id,match_time,score,income,outcome,profit,cdate");
        dimension = jobConf.getProperty("dimension", "product_id,user_id,score,income,outcome,cdate");
        hdfsPath = jobConf.getProperty("hdfsPath", "/hive/warehouse/yangyn.db/majiang_winpercentage_userlist/year=2017/month=03/");
        timeFormat = jobConf.getProperty("timeFormat", "YYYYMMdd");
        listDelimiter = jobConf.getProperty("listDelimiter", ",");
        classLoader = jobConf.getProperty("classLoader", "-javax.validation.,java.,javax.," +
                "org.apache.commons.logging.,org.apache.log4j.,org.apache.hadoop.,org.apache.xerces.,org.xml.sax.,org.w3c.dom.");


        Map<String, Object> res = new druidSchMaker().druidSchema();
        Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().disableHtmlEscaping().create();
        String resJson = gson.toJson(res);
        System.out.println(resJson);
    }

}
