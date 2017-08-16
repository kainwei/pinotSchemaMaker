import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by weizh on 2017/4/6.
 */
public class PinotCsvConfMaker {
    public static void main(String [] args ){

        Map<String, Object> res = new HashMap<String, Object>();



        //cdd
        //String colums = "id,pid,acc_num,acc_type,rel_pid,rel_acc_num,rel_acc_type,deal_date,deal_class_id,deal_id,deal_type_id,deal_subtype_id,deal_place_id,debit_currency_id,debit_amount,debit_balance,credit_currency_id,credit_amount,credit_balance,outer_account_id,note";
        String colums = "product_id,user_id,match_time,score,income,outcome,profit,cdate";

        res.put("CsvFileFormat", "EXCEL");
        res.put("CsvHeader",colums);
        res.put("CsvDelimiter",",");
        res.put("CsvDateFormat", "YYYYMMdd");

        Gson gson = new GsonBuilder().serializeNulls().create();
        String resJson = gson.toJson(res);
        System.out.println(resJson);
    }
}
