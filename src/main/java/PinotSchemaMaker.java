import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by weizh on 2017/3/9.
 */
public class PinotSchemaMaker {
    public static void PushToList(String [] src, List<Object> dst){
        for ( String s:src ){
            Map<String, Object> tmp = new HashMap<String, Object>();
            String [] tmpArr = s.split(":");
            tmp.put("name", tmpArr[0]);
            tmp.put("dataType", tmpArr[1].toUpperCase());
            tmp.put("delimiter",null);
            tmp.put("singleValueField", true);
            dst.add(tmp);
        }
        return;
    }

    public static List<Object> timeFieldSpec(String str){
        List<Object> res = new ArrayList<Object>();
        String [] arr = str.split(",");
        for(String s:arr){
            Map<String, String> tmp = new HashMap<String, String>();
            String [] subArr = s.split(":");
            tmp.put("timeType", subArr[0]);
            tmp.put("name", subArr[1]);
            tmp.put("dataType", subArr[2]);
            res.add(tmp);
        }
        return res;
    }

    public Map<String, Object> incomingGranularitySpec() {
        Map<String, Object> res = new HashMap<String, Object>();
        res.put("timeType", "DAYS");
        res.put("dataType", "LONG");
        res.put("name", "incomingName1");
        return res;
    }

    public Map<String, Object> outgoingGranularitySpec() {
        Map<String, Object> res = new HashMap<String, Object>();
        res.put("timeType", "DAYS");
        res.put("dataType", "LONG");
        res.put("name", "outgoingName1");
        return res;
    }

    public Map<String, Object> timeFieldSpec() {
        Map<String, Object> res = new HashMap<String, Object>();
        res.put("incomingGranularitySpec", this.incomingGranularitySpec());
        res.put("outgoingGranularitySpec", this.outgoingGranularitySpec());
        return res;
    }



    public static void main(String [] args ){
        String tablename = "majiang_WinPercentage_userlist";
        Map<String, Object> res = new HashMap<String, Object>();
        List<Object> dimension = new ArrayList<Object>();
        List<Object> metric = new ArrayList<Object>();
        //String dim = "userid:string,retain_date:string,last_week_liveness:int,last_2weeks_liveness:int,first_recharge_date:string,his_max_recharge_date:string,30d_max_recharge_date:string,30d_sum_recharge_amount:int,7d_profit_level_date:string,last_recharge_date:string,7d_profit_amount:int,7d_profit_level:int,his_contribution_degree_date:string,consume_level_date:string,consume_amount_7d:int";
        //cdd
        //String dim = "pid:int,id:int,acc_num:int,acc_type:int,rel_pid:int,rel_acc_num:int,rel_acc_type:int,deal_class_id:int,deal_id:int,deal_type_id:int,deal_subtype_id:int,deal_place_id:int,debit_currency_id:int,debit_amount:int,debit_balance:int,credit_currency_id:int,credit_amount:int,credit_balance:int,outer_account_id:int,note:string";
        String dim = "product_id:int,user_id:int,score:int,income:int,outcome:int,cdate:string";
        String [] dArr = dim.split(",");
        PushToList(dArr, dimension);

        //String mic = "last_week_liveness_days:int,last_2weeks_liveness_days:int,first_recharge_amount:float,his_max_recharge_amount:float,30d_max_recharge_amount:int,30d_sum_recharge_days:int,last_recharge_amount:int,7d_profit_game_days:int,his_contribution_degree:int,consume_level:int";
        //cdd
        //String mic = "deal_date:int";
        String mic = "match_time:double,profit:int";
        String [] mArr = mic.split(",");
        PushToList(mArr, metric);
        res.put("dimensionFieldSpecs", dimension);
        res.put("metricFieldSpecs", metric);
        //res.put("timeFieldSpec", new PinotSchemaMaker().timeFieldSpec());
        res.put("schemaName",tablename);


        Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
        String resJson = gson.toJson(res);
        System.out.println(resJson);
    }



}
