package com.feidee.bigdata.online.ad.feature;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.Node;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/7/4.
 */
public class FeatureProcessingHelp extends FeatureProcessing{
    public static String featureSparkModelRow(Map<String, Map<String, String>>config, String[] tableList, String[] idfeatureName, Row row, Integer sp, String pro, Map<String, String>filter, String modelLine){
        Map<String,String>featureValue = new HashMap<String,String>();
        Integer featureLength = tableList.length;
        String[] valueStrList = new String[featureLength];
        for (int i=0; i<featureLength; i++) {
            if (row.get(i) == null ) {
                valueStrList[i] = "NULL";
                continue;
            }
            String valueType = row.get(i).getClass().toString();
            if (valueType.equals("class java.lang.String")) {
                valueStrList[i] = row.getString(i);
            } else if (valueType.equals("class java.lang.Integer")) {
                valueStrList[i] = String.valueOf(row.getInt(i));
            } else if (valueType.equals("class java.lang.Long")) {
                valueStrList[i] = String.valueOf(row.getLong(i));
            } else if (valueType.equals("class java.math.BigDecimal")) {
                valueStrList[i] = String.valueOf(row.getDecimal(i));
            } else {
                valueStrList[i] = String.valueOf(row.getDouble(i));
            }
        }
        if(valueStrList.length != featureLength){
            return "";
        }
        for(Integer i = 0;i < featureLength; ++ i){
            featureValue.put(tableList[i], valueStrList[i]);
        }

        if(filter != null) {
            for (Map.Entry<String, String> entry : filter.entrySet()) {
                String filterKey = entry.getKey();
                if(!featureValue.containsKey(filterKey)) {
                    return "";
                }
                String filterValue = entry.getValue();
                if(!filterValue.equals(featureValue.get(filterKey))) {
                    return "";
                }
            }
        }

        String oneHotStr = "";
        if(featureValue.containsKey("clicktime")){
            String click = featureValue.get("clicktime");
            if(click.equals("NULL")){
                Integer showRandInt = (int)(Math.random() * sp);
                if(showRandInt != 0){
                    return "";
                } else {
                    oneHotStr = "0";
                }
            } else {
                oneHotStr = "1";
            }
        }else{
            return "";
        }

        HashMap<String,Double>featureColumnValue = new HashMap<String,Double>();
        FeatureProcessing.updateNameId(config);
        Integer ret = -1;
        if(pro.equals("gbdt")) {
            if (modelLine.equals("test")) {
                featureValue.put(ModelNameKey, ModelImageFeatureNameValue);
            } else {
                featureValue.put(ModelNameKey, "notFitGFroup");
                featureValue.put(testModelNameKey, testModelNameValue);
            }
            ret = getFeatureColumnValue(featureValue, featureColumnValue);
        } else if (pro.equals("gbdt+lr")) {
            if (modelLine.equals("test")) {
                featureValue.put(ModelNameKey, ModelImageFeatureNameValue);
            } else {
                featureValue.put(ModelNameKey, "notFitGFroup");
            }
            ret = getGBDTFeatureColumnValue(featureValue, featureColumnValue);
        } else if (pro.equals("ssjSQ+lr")) {
            ret = getSQFeatureColumnValue(featureValue, featureColumnValue);
        }else if (pro.equals("np+gbdt")  || pro.equals("loan+gbdt+lr")) {
            ret = getNPFeatureColumnValue(featureValue, featureColumnValue);
        }

        if(idfeatureName != null) {
            for (Integer i = 1; i < idfeatureName.length; i++) {
                String value = "0";
                if (featureColumnValue.containsKey(idfeatureName[i])) {
                    value = featureColumnValue.get(idfeatureName[i]).toString();
                }
                oneHotStr = oneHotStr + " " + value;
            }
        }
        return oneHotStr;
    }

    public static LabeledPoint featureSparkModelRowNew(Map<String, Map<String, String>>config, String[] tableList, Row row, Integer sp, String pro, Map<String, String>filter, String modelLine, String featureNameIdStr) {
        Map<String,String>featureValue = new HashMap<String,String>();
        Integer featureLength = tableList.length;
        String[] valueStrList = new String[featureLength];
        for (int i=0; i<featureLength; i++) {
            if (row.get(i) == null ) {
                valueStrList[i] = "NULL";
                continue;
            }
            String valueType = row.get(i).getClass().toString();
            if (valueType.equals("class java.lang.String")) {
                valueStrList[i] = row.getString(i);
            } else if (valueType.equals("class java.lang.Integer")) {
                valueStrList[i] = String.valueOf(row.getInt(i));
            } else if (valueType.equals("class java.lang.Long")) {
                valueStrList[i] = String.valueOf(row.getLong(i));
            } else if (valueType.equals("class java.math.BigDecimal")) {
                valueStrList[i] = String.valueOf(row.getDecimal(i));
            } else {
                valueStrList[i] = String.valueOf(row.getDouble(i));
            }
        }
        if (valueStrList.length != featureLength) {
            return new LabeledPoint(-1.0, Vectors.sparse(2, new int[1], new double[1]));
        }
        for(Integer i = 0;i < featureLength; ++ i){
            featureValue.put(tableList[i], valueStrList[i]);
        }

        if (filter != null) {
            for (Map.Entry<String, String> entry : filter.entrySet()) {
                String filterKey = entry.getKey();
                if (!featureValue.containsKey(filterKey)) {
                    return new LabeledPoint(-1.0, Vectors.sparse(2, new int[1], new double[1]));
                }
                String filterValue = entry.getValue();
                if (!filterValue.equals(featureValue.get(filterKey))) {
                    return new LabeledPoint(-1.0, Vectors.sparse(2, new int[1], new double[1]));
                }
            }
        }

        Integer lable = -1;
        if (featureValue.containsKey("clicktime")) {
            String click = featureValue.get("clicktime");
            if (click.equals("NULL")) {
                Integer showRandInt = (int) (Math.random() * sp);
                if (showRandInt != 0) {
                    return new LabeledPoint(-1.0, Vectors.sparse(2, new int[1], new double[1]));
                } else {
                    lable = 0;
                }
            } else {
                lable = 1;
            }
        } else {
            return new LabeledPoint(-1.0, Vectors.sparse(2, new int[1], new double[1]));
        }
        getRealTimeTag(featureValue);

        HashMap<String,Double>featureColumnValue = new HashMap<String,Double>();
        FeatureProcessing.updateNameId(config);
        Integer ret = -1;
        if (modelLine.equals("kn_np")) {
            ret = getNPFeatureColumnValue(featureValue, featureColumnValue);
        } else if(pro.equals("gbdt")) {
            if (modelLine.equals("test")) {
                featureValue.put(ModelNameKey, ModelImageFeatureNameValue);
            } else {
                featureValue.put(ModelNameKey, "notFitGFroup");
            }
            ret = getFeatureColumnValue(featureValue, featureColumnValue);
        } else if (pro.equals("gbdt+lr")) {
            if (modelLine.equals("test")) {
                featureValue.put(ModelNameKey, ModelImageFeatureNameValue);
            } else {
                featureValue.put(ModelNameKey, "notFitGFroup");
            }
            ret = getGBDTFeatureColumnValue(featureValue, featureColumnValue);
        } else if (pro.equals("ssjSQ+lr")) {
            ret = getSQFeatureColumnValue(featureValue, featureColumnValue);
        }

        Map<String, String> featureColumnId = config.get(featureNameIdStr);
        Integer len = featureColumnValue.size();
        double[] s_v = new double[len];
        int[] po = new int[len];
        int i = 0;
        for (Map.Entry<String, Double> entry : featureColumnValue.entrySet()) {
            if (featureColumnId.containsKey(entry.getKey())) {
                s_v[i] = entry.getValue();
                po[i] = Integer.parseInt(featureColumnId.get(entry.getKey())) - 1;
                i++;
            }
        }
        return new LabeledPoint(lable, Vectors.sparse(featureColumnId.size(), po, s_v));
    }

    public static Integer getRealTimeTag(Map<String,String>featureValue) {
        if(featureValue == null) {
            return 1;
        }
        if (!featureValue.containsKey("id_stat_list") || !featureValue.containsKey("viewtime") || !featureValue.containsKey("origid") || !featureValue.containsKey("accountid")) {
            return 2;
        }
        String origid = featureValue.get("origid");
        String accountid = featureValue.get("accountid");
        int origid_show = 0;
        int origid_click = 0;
        int accountid_show = 0;
        int accountid_click = 0;
        long viewtime = date2TimeStamp(featureValue.get("viewtime"));
        String[] id_list = featureValue.get("id_stat_list").split("&");
        for (int i = 0;i < id_list.length; ++ i) {
            String[] _list = id_list[i].split(",");
            if (_list.length < 3){
                continue;
            }
            long actiontime = date2TimeStamp(_list[2]);
            int click = 0;
            if(_list.length == 4) {
                click = 1;
            }
            if(actiontime < viewtime) {
                if (accountid.equals(_list[1])){
                    accountid_show += 1;
                    accountid_click += click;
                }
                if (origid.equals(_list[0])){
                    origid_show += 1;
                    origid_click += click;
                }
            }
        }
        featureValue.put("origidshow", String.valueOf(origid_show));
        featureValue.put("origidclick", String.valueOf(origid_click));
        if(origid_show > 0) {
            featureValue.put("origidctr", String.valueOf((float) origid_click / (float) origid_show));
        } else {
            featureValue.put("origidctr", "0.0");
        }
        featureValue.put("accountidshow", String.valueOf(accountid_show));
        featureValue.put("accountidclick", String.valueOf(accountid_click));
        if(accountid_show > 0) {
            featureValue.put("accountidctr", String.valueOf((float) accountid_click / (float) accountid_show));
        } else {
            featureValue.put("accountidctr", "0.0");
        }
        return 0;
    }
    public static long date2TimeStamp(String date_str){
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return sdf.parse(date_str).getTime();
        } catch (Exception e) {
            return 0;
        }
    }

    private static Integer getProductName(String valueProductName) {
        if (valueProductName == null) {
            return -1;
        }
        String valueLower = valueProductName.toLowerCase();
        if (valueLower.indexOf("mymoney") == 0){
            return 0;
        } else if (valueLower.indexOf("mycard") == 0 || valueLower.indexOf("cardniu") == 0) {
            return 1;
        } else if (valueLower.indexOf("dsp") == 0) {
            return 3;
        }
        return 2;
    }

    public static LabeledPoint getLabeledPoint(String s) {
        String [] li = s.split(" ");
        Integer feature_len = li.length - 1;
        Integer len = 0;
        for(int j = 0;j < feature_len; ++ j){
            if( Double.parseDouble(li[j + 1]) != 0.0 ){
                len++;
            }
        }
        double[] v = new double[len];
        int[] po = new int[len];
        int i = 0;
        for(int j = 0;j < feature_len; ++ j){
            double va = Double.parseDouble(li[j + 1]);
            if( va != 0.0 ){
                v[i] = va; po[i] = j; i ++;
            }
        }
        return new LabeledPoint(Double.parseDouble(li[0]), Vectors.sparse(feature_len, po, v));
    }

    public static int loadTableColumnName(String columnFile, ArrayList<String> tableColumnNameList)  throws IOException {
        InputStream is = new FileInputStream(columnFile);
        if(is == null | tableColumnNameList == null) {
            return 0;
        }
        BufferedReader reader = null;
        try{
            reader = new BufferedReader(new InputStreamReader(is));
            String lineString = null;
            while((lineString = reader.readLine()) != null) {
                String[] idNameList = lineString.split("\t", -1);
                if(idNameList.length == 2) {
                    String tableColumnName = idNameList[0];
                    tableColumnNameList.add(tableColumnName);
                } else {
                    return 3;
                }
            }
            if(lineString == null) {
                return 2;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 1;

    }

    public static String getAllNode(Node node, String treeId, List<String> ans){
        if(node.isLeaf()){
            ans.add(treeId + "_" + node.id());
            return node.id() + ",-1,-1,-1,-1";
        }
        String nodeString =  node.id() +
                "," + node.leftNode().get().id() +
                "," + node.rightNode().get().id() +
                "," + node.split().get().threshold() +
                "," + node.split().get().feature();
        return nodeString + "|" + getAllNode(node.leftNode().get(), treeId, ans) + "|" + getAllNode(node.rightNode().get(), treeId, ans);
    }

    public static void loadDisorderedValue(String fileName, Map<String, Map<String, String>> config) throws IOException {
        InputStream is = new FileInputStream(fileName);
        BufferedReader reader = null;
        try{
            reader = new BufferedReader(new InputStreamReader(is));
            String lineString = null;
            while((lineString = reader.readLine()) != null) {
                String[] idNameList = lineString.split("\t", -1);
                if(idNameList.length == 3) {
                    String type = idNameList[0];
                    String key = idNameList[1];
                    String value = idNameList[2];
                    if(config.containsKey(type) == false){
                        config.put(type, new HashMap<String, String>());
                    }
                    config.get(type).put(key, value);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void loadRunConfig(String fileName, Map<String, String> runConfig) throws IOException{
        InputStream istream = new FileInputStream(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(istream));
            String lineString = null;
            while ((lineString = reader.readLine()) != null) {
                String[] argsName = lineString.split("=");
                if (argsName.length == 2) {
                    System.out.println(argsName[0]+":"+argsName[1]);
                    runConfig.put(argsName[0], argsName[1]);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
