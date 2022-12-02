package com.feidee.bigdata.online.ad.feature;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 2017/4/27.
 */
public class SparkLRmodel extends FeatureProcessingHelp {
    public static String featureSparkModelLine(Map<String, Map<String, String>>config, String[] tableList, String[] idfeatureName, String line, Integer sp){
        Map<String,String>featureValue = new HashMap<String,String>();
        Integer featureLength = tableList.length;
        String[] valueStrList = line.split("\\001", -1);
        if(valueStrList.length != featureLength){
            return "";
        }
        for(Integer i = 0;i < featureLength; ++ i){
            featureValue.put(tableList[i], valueStrList[i]);
        }

        Integer randInt = (int)(Math.random()*10);
        String oneHotStr = randInt.toString();
        if(featureValue.containsKey("clicktime")){
            String click = featureValue.get("clicktime");
            if(click.equals("\\N") == true){
                Integer showRandInt = (int)(Math.random() * sp);
                if(showRandInt != 0){
                    return "";
                } else {
                    oneHotStr = oneHotStr + " 0";
                }
            } else {
                oneHotStr = oneHotStr + " 1";
            }
        }else{
            return "";
        }

        HashMap<String,Double>featureColumnValue = new HashMap<String,Double>();
        FeatureProcessing.updateNameId(config);

        Integer ret = getFeatureColumnValue(featureValue, featureColumnValue);
        if(idfeatureName != null) {
            for (Integer i = 1; i < idfeatureName.length; i++) {
                String value = "0";
                if (featureColumnValue.containsKey(idfeatureName[i])) {
                    String valueDouble = featureColumnValue.get(idfeatureName[i]).toString();
                    value = valueDouble.substring(0, valueDouble.indexOf("."));
                }
                oneHotStr = oneHotStr + " " + value;
            }
        }
        return oneHotStr;
    }
    public static void main(String[] args) throws IOException {
        final Map<String, Map<String, String>> config = new HashMap<>();
        final FeatureProcessing fp = new FeatureProcessing();
        loadDisorderedValue(args[2], config);

        Map<String, String> featureNameIdTemp = new ConcurrentHashMap<>();
        String[] tableList = null;
        if(args[3].equals("ssj")) {
            featureNameIdTemp = config.get(featureNameIdKeyStrSsj);
            tableList = new String[]{"requestid","umd5","did","ver","systemname","systemversion","productname","productversion","model","partner","username","channelsys","size","planid","origid","positionid","positionindex","viewtime","testcode","groupname","accountid","ymd","clicktime","groupid","_c0","udid","sid","userssj_ssjid","devssj_is_student","userssj_fn_buy_zc_frequency","userssj_ssj_latestpartner","userssj_zq_fin_user","userssj_is_ec_cost","userssj_abroad_fmon_cost","userssj_ssj_u_house","devssj_ssj_install_con_p2p","devssj_ssj_install_con_fin","userssj_ssj_no_income_user","userssj_fn_is_buyxsb_user","userssj_jzc_30d_amt_level","devssj_ssj_install_con_daikuan","devssj_is_spd","devssj_is_cmb","devssj_is_bcom","devssj_is_cgb","devssj_is_cib","devssj_is_cncb","userssj_sq_pv30d","userssj_fn_fbbinddays","userssj_zq_card_user","userssj_ssj_credit_card_user","userssj_ssj_catbook_time","_c28","userssj_ssj_u_car","devssj_stock_app_instl","devssj_ins_ms_click_30d","devssj_credit_ms_click_30d","devssj_ssj_install_con_banka","devssj_ssj_install_con_baoxian","userssj_fin_jjisbuy_user","userssj_ssj_asset_level","devssj_ms_rec_30d","userssj_ssj_age","userssj_ssj_fin_user_period_level","userssj_ssj_sex","userssj_ssj_pv30d","devssj_ssj_inactive_days","userssj_ssj_is_lottery_user","userssj_ssj_fin_sleep_user","userssj_ssj_invest_acct_user","userssj_ssj_trlentity","_c47","userssj_ssj_regular_income_true","devssj_ms_del_fn_30d","devssj_ms_clk_fn_30d","userssj_ssj_regular_income","devssj_sp_clk_30d","devssj_ms_clk_30d","userssj_ssj_sp_explastday_30d","userssj_sq_user","userssj_fn_fbuser","userssj_ssj_fn_jjuser","userssj_ssj_activedays","userssj_fn_fbisbuy","userssj_if_p2p_in90d","userssj_if_fnpf_in90d","userssj_fn_is_feibiaobind","userssj_ssj_vfinancepv30d","userssj_ssj_reg_dayage","userssj_if_investrade_near90d","userssj_if_businessentity","devssj_mostcity_180d","devssj_mostprovince_180d","devssj_nwdmost_90d_type","userssj_yx_ssj_clickxxt_30d","userssj_yx_ssj_clickzbtt_30d","userssj_yx_ssj_clicksqb_30d","userssj_yx_ssj_clickdft_30d","userssj_yx_ssj_showxxt_30d","userssj_yx_ssj_showzbtt_30d","userssj_yx_ssj_showsqb_30d","userssj_yx_ssj_showcpb_30d","userssj_yx_ssj_showdft_30d","userssj_yx_ssj_showsqhlb_30d","userssj_yx_ssj_showsqhlft_30d","userssj_yx_ssj_showsqhlbpb_30d","num"};
        }
        Integer featureColumnLen = featureNameIdTemp.size();
        final String[] idfeatureName = new String[featureColumnLen + 1];
        for(Map.Entry<String, String> entry : featureNameIdTemp.entrySet()){
            idfeatureName[Integer.parseInt(entry.getValue())] = entry.getKey();
        }

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> [] splits = sc.textFile(args[0]).randomSplit(new double[] {0.9, 0.1});

        final String[] finalTableList = tableList;
        JavaRDD<LabeledPoint> trainData = splits[0].map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return featureSparkModelLine(config, finalTableList, idfeatureName, s, 100);
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.equals("");
            }
        }).map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String s) throws Exception {
                String [] li = s.split(" ");
                Integer feature_len =  li.length - 2;
                double[] v = new double[feature_len];
                for(int j = 0;j < feature_len; ++ j){
                    v[j] = Double.parseDouble(li[j + 2]);
                }
                return new LabeledPoint(Double.parseDouble(li[1]), Vectors.dense(v));
            }
        }).cache();

        JavaRDD<LabeledPoint> testData = splits[1].map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return featureSparkModelLine(config, finalTableList, idfeatureName, s, 100);
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.equals("");
            }
        }).map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String s) throws Exception {
                String [] li = s.split(" ");
                Integer feature_len =  li.length - 2;
                double[] v = new double[feature_len];
                for(int j = 0;j < feature_len; ++ j){
                    v[j] = Double.parseDouble(li[j + 2]);
                }
                return new LabeledPoint(Double.parseDouble(li[1]), Vectors.dense(v));
            }
        });

        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainData.rdd());

        model.clearThreshold();

        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = testData.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictionAndLabels.rdd());
        double auroc = metrics.areaUnderROC();
        double auprc = metrics.areaUnderPR();
        String str = "AUROC:" + auroc + "   auprc:" + auprc;
        String model_str = model.weights().toString();
        List<String> strList = new ArrayList<String>();
        strList.add(str);
        strList.add(model_str);
        JavaRDD<String>rddList = sc.parallelize(strList);
        rddList.saveAsTextFile(args[1]);
    }
}
