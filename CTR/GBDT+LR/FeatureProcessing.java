package com.feidee.bigdata.online.ad.feature;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;


/**
 * Created by dongjie on 2017/4/7.
 * ctr预估特征处理
 */
public class FeatureProcessing {
    protected static final String featureNameIdKeyStrSsj = "feature_name_id_ssj";
    protected static final String featureNameKeyStrSsj = "feature_name_list_ssj";
    protected static final String featureCrossoverKeyStrSsj = "feature_crossover_function_ssj";
    protected static final String ssjModelTreesStr = "ssj_model_trees";
    protected static final String featureNameIdKeyStrKnGbdt = "feature_name_id_kn_gbdt";
    protected static final String featureNameKeyStrKnGbdt = "feature_name_list_kn_gbdt";
    protected static final String featureCrossoverKeyStrKnGbdt = "feature_crossover_function_kn_gbdt";
    protected static final String knModelTreesStr = "kn_model_trees";
    protected static final String featureNameIdKeyStrKnGbdtNew = "feature_name_id_kn_gbdt_new";
    protected static final String featureNameKeyStrKnGbdtNew = "feature_name_list_kn_gbdt_new";
    protected static final String featureCrossoverKeyStrKnGbdtNew = "feature_crossover_function_kn_gbdt_new";
    protected static final String knModelTreesNewStr = "kn_model_trees_new";
    protected static final String knModelOrigIdTreeNameIdSuff = "origid_trees_";
    protected static final String ModelNameIdSuff = "trees_";
    private static final String functionNameGetValue = "getValue";
    private static final String functionNameGetValue2 = "getValue2";
    private static final String functionNameGetOrigidSta = "getOrigidSta";
    private static final String functionNameGetAccountidSta = "getAccountidSta";
    private static final String functionNameGetDiscretization = "getFeatureDiscretization";
    private static final String functionNameOrigidCtr = "origidCtr";
    private static final String functionNameAccountidCtr = "accountidCtr";
    private static final String functionNameSystem = "systemNameMapping";
    private static final String functionNameMapping = "featureColumnNameMaping";
    private static final String functionListMapping = "featureColumnListMaping";
    private static final String functionNameHour = "showTimeMapping";
    private static final String functionNameYxStatistics = "yxStatisticsMapping";
    private static final String productNameStr = "productname";
    private static final String positionidStr = "positionid";
    protected static final String testModelNameKey = "CtrModel";
    protected static final String testModelNameValue = "GBDT";
    protected static final String ModelNameKey = "groupname";
    protected static final String ModelImageFeatureNameValue = "CTR_GBDT_Model_IMAGE_DJ";
    protected static final String useridOrigidSim = "userid_origid_sim";
    protected static final String origidKeyName = "origid";
    protected static Map<String, String> featureColumnIdSsj;
    protected static Map<String, String> featureNameListSsj;
    protected static Map<String, String> featureCrossoverFunctionSsj;
    protected static Map<String, String> featureColumnIdKnGbdt;
    protected static Map<String, String> featureNameListKnGbdt;
    protected static Map<String, String> featureCrossoverFunctionKnGbdt;
    protected static Map<String, TreeNode[]> knModelTreeList;
    protected static Map<String, String> featureColumnIdKnGbdtNew;
    protected static Map<String, String> featureNameListKnGbdtNew;
    protected static Map<String, String> featureCrossoverFunctionKnGbdtNew;
    protected static Map<String, TreeNode[]> knModelTreeListNew;
    protected static Map<String, Map<String, TreeNode[]>> knModelIDTreeListNew;
    protected static Map<String, TreeNode[]> ssjModelTreeList;
    protected static final String featureNameIdKeyStr = "feature_name_id";
    protected static final String featureFunKeyStr = "feature_name_list";
    protected static final String featureCrossoverKeyStr = "feature_crossover_function";
    protected static final String featureDiscretizationKeyStr = "feature_discretization";
    protected static final String TreesKeyStr = "model_trees";
    protected static final String ssjConfigNewSuff = "ssj_gbdt_new";
    protected static Map<String, Map<String, String>> ssjModelConfigNew;
    protected static Map<String,TreeNode[]> ssjModelTreeListNew;
    protected  static final String ssjSQConfigSuff = "ssjSQ";
    protected static Map<String,Map<String,String>> ssjSQModelConfig;
    protected static final String knSpConfigSuff = "knsp_gbdt";
    protected static Map<String, Map<String,String>> knSpModelConfig;
    protected static Map<String, TreeNode[]> knSpModelTreeList;
    protected static final String npConfigSuff = "kn_new_period";
    protected static final String npModelTreesStr = "kn_new_period_model_trees";
    protected static Map<String, Map<String,String>> npModelConfig;
    protected static Map<String, TreeNode[]> npModelTreeList;

    public static Map<String, String> getfeatureColumnIdKnGbdtNew(){
        return featureColumnIdKnGbdtNew;
    }

    private static Integer isConfigValueNull(Map<String, String> conf) {
        for (Map.Entry<String,String> c : conf.entrySet()) {
            if (c.getKey() == null || c.getValue() == null) {
                return -1;
            }
        }
        return 0;
    }
    /**
     *
     * @param config mysql配置数据
     * @return 0：正常
     */
    public static Integer updateNameId(Map<String, Map<String, String>> config){
        if(config == null) {
            return 1;
        }
        Integer ret = 0;
        for (Map.Entry<String,Map<String, String>> entry : config.entrySet()) {
            if (entry.getValue() == null) {
                return -1;
            }
        }

        if(featureColumnIdSsj == null) {
            featureColumnIdSsj = new ConcurrentHashMap<String, String>();
        }
        featureColumnIdSsj.clear();
        if(config.containsKey(featureNameIdKeyStrSsj)){
            if (isConfigValueNull(config.get(featureNameIdKeyStrSsj)) == -1) {
                ret = ret + 2;
            } else {
                featureColumnIdSsj.putAll(config.get(featureNameIdKeyStrSsj));
            }
        }else{
            ret = ret + 2;
        }

        if(featureNameListSsj == null){
            featureNameListSsj = new ConcurrentHashMap<String, String>();
        }
        featureNameListSsj.clear();
        if (config.containsKey(featureNameKeyStrSsj)){
            if (isConfigValueNull(config.get(featureNameKeyStrSsj)) == -1) {
                ret = ret + 4;
            } else {
                featureNameListSsj.putAll(config.get(featureNameKeyStrSsj));
            }
        }else{
            ret = ret + 4;
        }

        if(featureCrossoverFunctionSsj == null){
            featureCrossoverFunctionSsj = new ConcurrentHashMap<String, String>();
        }
        featureCrossoverFunctionSsj.clear();
        if (config.containsKey(featureCrossoverKeyStrSsj)){
            if (isConfigValueNull(config.get(featureCrossoverKeyStrSsj)) == -1) {
                ret = ret + 8;
            } else {
                featureCrossoverFunctionSsj.putAll(config.get(featureCrossoverKeyStrSsj));
            }
        }else{
            ret = ret + 8;
        }

        if(knModelTreeList == null){
            knModelTreeList = new ConcurrentHashMap<>();
        }
        knModelTreeList.clear();
        if (config.containsKey(knModelTreesStr)){
            if (isConfigValueNull(config.get(knModelTreesStr)) == -1) {
                ret = ret + 128;
            } else {
                Map<String, String> knModelTreesConfig = config.get(knModelTreesStr);
                createTree(knModelTreesConfig, knModelTreeList);
            }
        }else{
            ret = ret + 128;
        }

        if(featureColumnIdKnGbdt == null) {
            featureColumnIdKnGbdt = new ConcurrentHashMap<String, String>();
        }
        featureColumnIdKnGbdt.clear();
        if(config.containsKey(featureNameIdKeyStrKnGbdt)){
            if (isConfigValueNull(config.get(featureNameIdKeyStrKnGbdt)) == -1) {
                ret = ret + 256;
            } else {
                featureColumnIdKnGbdt.putAll(config.get(featureNameIdKeyStrKnGbdt));
            }
        }else{
            ret = ret + 256;
        }

        if(featureNameListKnGbdt == null){
            featureNameListKnGbdt = new ConcurrentHashMap<String, String>();
        }
        featureNameListKnGbdt.clear();
        if (config.containsKey(featureNameKeyStrKnGbdt)){
            if (isConfigValueNull(config.get(featureNameKeyStrKnGbdt)) == -1) {
                ret = ret + 512;
            } else {
                featureNameListKnGbdt.putAll(config.get(featureNameKeyStrKnGbdt));
            }
        }else{
            ret = ret + 512;
        }

        if(featureCrossoverFunctionKnGbdt == null){
            featureCrossoverFunctionKnGbdt = new ConcurrentHashMap<String, String>();
        }
        featureCrossoverFunctionKnGbdt.clear();
        if (config.containsKey(featureCrossoverKeyStrKnGbdt)){
            if (isConfigValueNull(config.get(featureCrossoverKeyStrKnGbdt)) == -1) {
                ret = ret + 1024;
            } else {
                featureCrossoverFunctionKnGbdt.putAll(config.get(featureCrossoverKeyStrKnGbdt));
            }
        }else{
            ret = ret + 1024;
        }

        if(knModelTreeListNew == null){
            knModelTreeListNew = new ConcurrentHashMap<>();
        }
        knModelTreeListNew.clear();
        if (config.containsKey(knModelTreesNewStr)){
            if (isConfigValueNull(config.get(knModelTreesNewStr)) == -1) {
                ret = ret + 16384;
            } else {
                Map<String, String> knModelTreesConfig = config.get(knModelTreesNewStr);
                createTree(knModelTreesConfig, knModelTreeListNew);
            }
        }else{
            ret = ret + 16384;
        }

        if(featureColumnIdKnGbdtNew == null) {
            featureColumnIdKnGbdtNew = new ConcurrentHashMap<String, String>();
        }
        featureColumnIdKnGbdtNew.clear();
        if(config.containsKey(featureNameIdKeyStrKnGbdtNew)){
            if (isConfigValueNull(config.get(featureNameIdKeyStrKnGbdtNew)) == -1) {
                ret = ret + 32768;
            } else {
                featureColumnIdKnGbdtNew.putAll(config.get(featureNameIdKeyStrKnGbdtNew));
            }
        }else{
            ret = ret + 32768;
        }

        if(featureNameListKnGbdtNew == null){
            featureNameListKnGbdtNew = new ConcurrentHashMap<String, String>();
        }
        featureNameListKnGbdtNew.clear();
        if (config.containsKey(featureNameKeyStrKnGbdtNew)){
            if (isConfigValueNull(config.get(featureNameKeyStrKnGbdtNew)) == -1) {
                ret = ret + 65536;
            } else {
                featureNameListKnGbdtNew.putAll(config.get(featureNameKeyStrKnGbdtNew));
            }
        }else{
            ret = ret + 65536;
        }

        if(featureCrossoverFunctionKnGbdtNew == null){
            featureCrossoverFunctionKnGbdtNew = new ConcurrentHashMap<String, String>();
        }
        featureCrossoverFunctionKnGbdtNew.clear();
        if (config.containsKey(featureCrossoverKeyStrKnGbdtNew)){
            if (isConfigValueNull(config.get(featureCrossoverKeyStrKnGbdtNew)) == -1) {
                ret = ret + 131072;
            } else {
                featureCrossoverFunctionKnGbdtNew.putAll(config.get(featureCrossoverKeyStrKnGbdtNew));
            }
        }else{
            ret = ret + 131072;
        }

        ssjModelConfigNew = new ConcurrentHashMap<>();
        int ssjModelNewRet = updateModelConfig(config, ssjConfigNewSuff, ssjModelConfigNew);
        if (ssjModelNewRet == -1 || ssjModelNewRet == 1 || ssjModelNewRet == 2) {
        	ret = ret + 2097152;
        }
        String ssjModelTreeKeyStr = TreesKeyStr + "_" + ssjConfigNewSuff;
        ssjModelTreeListNew = new ConcurrentHashMap<>();
        if (config.containsKey(ssjModelTreeKeyStr)) {
            if (isConfigValueNull(config.get(ssjModelTreeKeyStr)) == -1) {
                ret = ret + 4194304;
            } else {
                Map<String,String> ssjModelTreesConfig = config.get(ssjModelTreeKeyStr);
                createTree(ssjModelTreesConfig,ssjModelTreeListNew);
            }
        } else {
            ret = ret + 4194304;
        }

        ssjSQModelConfig = new ConcurrentHashMap<>();
        int ssjSQModelRet = updateModelConfig(config,ssjSQConfigSuff,ssjSQModelConfig);
        if (ssjSQModelRet == -1 || ssjSQModelRet == 1 || ssjSQModelRet == 2) {
            ret = ret + 8388608;
        }

        knSpModelConfig = new ConcurrentHashMap<>();
        int knSpModelRet = updateModelConfig(config,knSpConfigSuff,knSpModelConfig);
        if (knSpModelRet == -1 || knSpModelRet == 1 || knSpModelRet == 2) {
            ret = ret + 16777216;
        }
        knSpModelTreeList = new ConcurrentHashMap<>();
        if (config.containsKey(TreesKeyStr + "_" + knSpConfigSuff)) {
            if (isConfigValueNull(config.get(TreesKeyStr + "_" + knSpConfigSuff)) == -1) {
                ret = ret + 33554432;
            } else {
                Map<String,String> knSpModelTreesConfig = config.get(TreesKeyStr + "_" + knSpConfigSuff);
                createTree(knSpModelTreesConfig,knSpModelTreeList);
            }
        } else {
            ret = ret + 33554432;
        }
        npModelConfig = new ConcurrentHashMap<>();
        int npModelRet = updateModelConfig(config, npConfigSuff, npModelConfig);
        if (npModelRet == -1 || npModelRet == 1 || npModelRet == 2) {
            ret = ret + 67108864;
        }

        if(ssjModelTreeList == null){
            ssjModelTreeList = new ConcurrentHashMap<>();
        }
        ssjModelTreeList.clear();
        if (config.containsKey(ssjModelTreesStr)){
            if (isConfigValueNull(config.get(ssjModelTreesStr)) == -1) {
                ret = ret + 134217728;
            } else {
                Map<String, String> knModelTreesConfig = config.get(ssjModelTreesStr);
                createTree(knModelTreesConfig, ssjModelTreeList);
            }
        }else{
            ret = ret + 134217728;
        }

        if(npModelTreeList == null){
            npModelTreeList = new ConcurrentHashMap<>();
        }
        npModelTreeList.clear();
        if (config.containsKey(npModelTreesStr)){
            if (isConfigValueNull(config.get(npModelTreesStr)) == -1) {
                ret = ret + 268435456;
            } else {
                Map<String, String> knModelTreesConfig = config.get(npModelTreesStr);
                createTree(knModelTreesConfig, npModelTreeList);
            }
        }else{
            ret = ret + 268435456;
        }
        return ret;
    }

    public static void unitConfigClear(){
        featureColumnIdSsj = null;
        featureNameListSsj = null;
        featureCrossoverFunctionSsj = null;
        featureColumnIdKnGbdt = null;
        featureNameListKnGbdt = null;
        featureCrossoverFunctionKnGbdt = null;
        knModelTreeList = null;
        featureColumnIdKnGbdtNew = null;
        featureNameListKnGbdtNew = null;
        featureCrossoverFunctionKnGbdtNew = null;
        knModelTreeListNew = null;
        ssjModelConfigNew = null;
        ssjModelTreeListNew = null;
        ssjSQModelConfig = null;
        knSpModelConfig = null;
        knSpModelTreeList = null;
        npModelConfig = null;
    }

    /**
     *
     * @param modelName 需要更新配置的模型名
     * @return
     */
    private static Integer updateModelConfig(Map<String, Map<String, String>> config, String modelName, Map<String, Map<String, String>>modelConfig){
        if(config == null || modelConfig == null) {
            return -1;
        }
        modelConfig.clear();
        String nameIdStr = featureNameIdKeyStr + "_" + modelName;
        if (config.containsKey(nameIdStr)) {
            if (isConfigValueNull(config.get(nameIdStr)) == -1) {
                return -1;
            }
            Map<String, String>nameIdMap = new ConcurrentHashMap<>(config.get(nameIdStr));
            modelConfig.put(nameIdStr, nameIdMap);
        } else {
            return 1;
        }

        String featureFunStr = featureFunKeyStr + "_" + modelName;
        if (config.containsKey(featureFunStr)) {
            if (isConfigValueNull(config.get(featureFunStr)) == -1) {
                return -1;
            }
            Map<String, String>featureFunMap = new ConcurrentHashMap<>(config.get(featureFunStr));
            modelConfig.put(featureFunStr, featureFunMap);
        } else {
            return 2;
        }

        int ret = 0;
        String featureCrossStr = featureCrossoverKeyStr + "_" + modelName;
        if (config.containsKey(featureCrossStr)) {
            if (isConfigValueNull(config.get(featureCrossStr)) == -1) {
                ret = -1;
            } else {
                Map<String, String> featureCrossMap = new ConcurrentHashMap<>(config.get(featureCrossStr));
                modelConfig.put(featureCrossStr, featureCrossMap);
            }
        }
        String featureDiscretizationStr = featureDiscretizationKeyStr + "_" + modelName;
        if (config.containsKey(featureDiscretizationStr)) {
            if (isConfigValueNull(config.get(featureDiscretizationStr)) == -1) {
                ret = -1;
            } else {
                Map<String, String> featureDiscretizationMap = new ConcurrentHashMap<>(config.get(featureDiscretizationStr));
                modelConfig.put(featureDiscretizationStr, featureDiscretizationMap);
            }
        }
        return ret;
    }

    private  static Integer createTree(Map<String, String> config, Map<String, TreeNode[]> knModelTree){
        if(config == null || knModelTree == null){
            return 1;
        }
        for (Map.Entry<String, String>entry : config.entrySet()) {
            String treeId = entry.getKey();
            String[] nodeStringList = entry.getValue().split("\\|");

            Integer nodeSize = nodeStringList.length;
            Integer maxLength = (int) (Math.pow(2, nodeSize / 2 + 1 )) - 1;
            Integer maxNodeId = 1;
            for(String nodeStr:nodeStringList) {
                String [] valueSplit = nodeStr.split(",");
                if(valueSplit.length != 5) {
                    continue;
                }
            	if( StringUtils.isNumeric(valueSplit[0]) ) {
            		if (Integer.parseInt(valueSplit[0]) > maxNodeId) {
                	    maxNodeId = Integer.parseInt(valueSplit[0]);
            		}
            	}
            }
            if(maxNodeId > maxLength) {
            	continue;
            }
            TreeNode [] nodelist = new TreeNode[maxNodeId + 1];
            for(String nodeStr:nodeStringList) {
                TreeNode n = new TreeNode(nodeStr);
                if(n.getNodeId() > 0) {
                	nodelist[n.getNodeId()] = n;
                }
            }
            if(nodelist[1] != null) {
                knModelTree.put(treeId, nodelist);
            }
        } 
        return 0;
    }

    private  static Integer createIDTree(Map<String, String> config, Map<String, Map<String, TreeNode[]>> knModelIDTree){
        if(config == null || knModelIDTree == null){
            return 1;
        }
        for (Map.Entry<String, String>entry : config.entrySet()) {
            String[] idValueList = entry.getKey().split("\\|");
            if(idValueList.length != 2){
                continue;
            }
            if(knModelIDTree.containsKey(idValueList[0]) == false){
                knModelIDTree.put(idValueList[0],  new ConcurrentHashMap<String, TreeNode[]>());
            }
            String[] nodeStringList = entry.getValue().split("\\|");
            Integer nodeSize = nodeStringList.length;
            TreeNode [] nodelist = new TreeNode[nodeSize + 10];
            for(String nodeStr:nodeStringList) {
                TreeNode n = new TreeNode(nodeStr);
                nodelist[n.getNodeId()] = n;
            }
            knModelIDTree.get(idValueList[0]).put(idValueList[1], nodelist);
        }
        return 0;
    }

    public static Integer getFeatureColumnValue(Map<String,String>featureValue, Map<String,Double> featureColumnValue) {
        if (featureValue == null) {
            return 1001;
        }
        if (featureColumnValue == null) {
            return 1002;
        }
        Map<String, String> featureNameList = null;
        Map<String, String> featureCrossoverFunction = null;
        Map<String, String> featureColumnId = null;
        Map<String, String> featureDiscretizationValue = null;
        if (featureValue.containsKey(productNameStr)) {
            String valueProductName = featureValue.get(productNameStr);
            if (valueProductName == null) {
                return 1004;
            }
            if (getProductName(valueProductName) == 0) {
            	String modelNameKeyStr = featureValue.get(ModelNameKey);
                if(modelNameKeyStr == null ) {
                    modelNameKeyStr = "";
                }
                if (modelNameKeyStr.equals(ModelImageFeatureNameValue)) {
                    if (ssjModelConfigNew == null) {
                        return 1006;
                    }
                    featureNameList = ssjModelConfigNew.get(featureFunKeyStr + "_" + ssjConfigNewSuff);
                    featureColumnId = ssjModelConfigNew.get(featureNameIdKeyStr + "_" + ssjConfigNewSuff);
                    featureCrossoverFunction = ssjModelConfigNew.get(featureCrossoverKeyStr + "_" + ssjConfigNewSuff);
                } else {
                    featureNameList = featureNameListSsj;
                    featureColumnId = featureColumnIdSsj;
                    featureCrossoverFunction = featureCrossoverFunctionSsj;
                }
            } else if (getProductName(valueProductName) == 1 ) {
                String modelNameKeyStr  = featureValue.get(ModelNameKey);
                if(modelNameKeyStr == null) {
                    modelNameKeyStr = "";
                }
                if (modelNameKeyStr.equals(ModelImageFeatureNameValue)) {
                    String positionIdStr = "";
                    if (featureValue.containsKey(positionidStr)) {
                        if (featureValue.get(positionidStr) != null){
                            positionIdStr = featureValue.get(positionidStr);
                        }
                    }
                    if (positionIdStr.equals("SP")) {
                        if (knSpModelConfig == null) {
                            return 1007;
                        }
                        featureNameList = knSpModelConfig.get(featureFunKeyStr + "_" + knSpConfigSuff);
                        featureColumnId = knSpModelConfig.get(featureNameIdKeyStr + "_" + knSpConfigSuff);
                        featureCrossoverFunction = knSpModelConfig.get(featureCrossoverKeyStr + "_" + knSpConfigSuff);
                        featureDiscretizationValue = knSpModelConfig.get(featureDiscretizationKeyStr+ "_" + knSpConfigSuff);
                    } else {
                        featureNameList = featureNameListKnGbdtNew;
                        featureColumnId = featureColumnIdKnGbdtNew;
                        featureCrossoverFunction = featureCrossoverFunctionKnGbdtNew;
                        featureColumnValue.put(useridOrigidSim, getUseridOrigidSim(featureValue));
                    }

                } else if (featureValue.containsKey(testModelNameKey)) {
                    if (featureValue.get(testModelNameKey).equals(testModelNameValue)) {
                        featureNameList = featureNameListKnGbdt;
                        featureColumnId = featureColumnIdKnGbdt;
                        featureCrossoverFunction = featureCrossoverFunctionKnGbdt;
                    }
                }
            } else {
                return 1005;
            }
        } else {
            return 1003;
        }

        Map<String, String> featureKV = new ConcurrentHashMap<>();
        if (featureColumnId == null) {
            return 1009;
        }
        if (featureNameList == null) {
            return 1008;
        }
        Integer singleFeatureRet = getSingleFeature(featureNameList, featureValue, featureColumnId, featureColumnValue, featureKV,featureDiscretizationValue);
        if (singleFeatureRet != 0) {
            return 1100+singleFeatureRet;
        }
        if (featureCrossoverFunction != null) {
            Integer crossoverRet = getfeatureCrossover(featureCrossoverFunction, featureKV, featureColumnValue);
        }
        return 0;
    }

    public static Integer getSQFeatureColumnValue(Map<String,String>featureValue, Map<String,Double> featureColumnValue) {
        if (featureValue == null) {
            return 1;
        }
        if (featureColumnValue == null) {
            return 2;
        }
        Map<String, String> featureNameList = null;
        Map<String, String> featureCrossoverFunction = null;
        Map<String, String> featureColumnId = null;
        if (featureValue.containsKey(productNameStr)) {
            String valueProductName = featureValue.get(productNameStr);
            if (valueProductName == null) {
                return 4;
            }
            if (getProductName(valueProductName) == 0) {
                featureNameList = ssjSQModelConfig.get(featureFunKeyStr + "_" + ssjSQConfigSuff);
                featureColumnId = ssjSQModelConfig.get(featureNameIdKeyStr + "_" + ssjSQConfigSuff);
                featureCrossoverFunction = ssjSQModelConfig.get(featureCrossoverKeyStr + "_" + ssjSQConfigSuff);
            } else if (getProductName(valueProductName) == 1 ) {
            }
        }
        Map<String, String> featureKV = new ConcurrentHashMap<>();

        Integer singleFeatureRet = getSingleFeature(featureNameList, featureValue, featureColumnId, featureColumnValue, featureKV,null);
        if (singleFeatureRet != 0) {
            return 3;
        }
        getfeatureCrossover(featureCrossoverFunction, featureKV, featureColumnValue);
        return 0;
    }

    public static Integer getNPFeatureColumnValue(Map<String,String>featureValue, Map<String,Double> featureColumnValue) {
        if (featureValue == null) {
            return 20001;
        }
        if (featureColumnValue == null) {
            return 20002;
        }
        Map<String, String> featureNameList = null;
        Map<String, String> featureCrossoverFunction = null;
        Map<String, String> featureColumnId = null;
        Map<String, TreeNode[]> ModelTreeListValue = null;
        if (npModelConfig == null) {
            return 20003;
        }
        featureNameList = npModelConfig.get(featureFunKeyStr + "_" + npConfigSuff);
        featureColumnId = npModelConfig.get(featureNameIdKeyStr + "_" + npConfigSuff);
        featureCrossoverFunction = npModelConfig.get(featureCrossoverKeyStr + "_" + npConfigSuff);
        ModelTreeListValue = npModelTreeList;
        Map<String, String> featureKV = new ConcurrentHashMap<>();
        if(featureNameList == null || featureColumnId == null){
            return 20004;
        }

        Integer singleFeatureRet = getSingleFeature(featureNameList, featureValue, featureColumnId, featureColumnValue, featureKV, null);
        if (singleFeatureRet != 0) {
            return 21000 + singleFeatureRet;
        }
        getfeatureCrossover(featureCrossoverFunction, featureKV, featureColumnValue);

        int featureColumnIdSize = featureColumnId.size() + 1;
        double[] features = new double[featureColumnIdSize];
        if (ModelTreeListValue != null) {
            getFeaturesList(featureColumnId, featureColumnValue, features);
        }
        getTreeFeature(features, ModelTreeListValue, featureColumnValue);
        return 0;
    }

    private static Integer getSingleFeature(Map<String, String>featureNameList, Map<String, String> featureValue,Map<String, String> featureColumnId, Map<String, Double>featureColumnValue, Map<String, String> featureKV, Map<String,String>featureDiscretizationValue){
        if (featureNameList == null || featureColumnId == null || featureValue == null || featureColumnValue == null || featureKV == null) {
            return 3;
        }

        for (Map.Entry<String, String> entry : featureNameList.entrySet()) {
            String featureName = entry.getKey();
            String featureProcessingFunName = entry.getValue();
            Double value = 0.0;
            String featureValueStr = "";
            if (featureValue.containsKey(featureName)) {
                featureValueStr = featureValue.get(featureName);
                if (featureValueStr == null) {
                    featureValueStr = "";
                }
            }
            String featureColumnName = entry.getKey();
            Map<String, Double> featureColumnListMap = new HashMap<String, Double>();
            String maxStr = "100.0";
            String minStr = "0.0";
            if (featureProcessingFunName.indexOf(functionNameGetValue2)==0) {
                maxStr = featureProcessingFunName.split("-")[1];
                minStr = featureProcessingFunName.split("-")[2];
                featureProcessingFunName = featureProcessingFunName.split("-")[0];
            }
            switch (featureProcessingFunName) {
                case functionNameGetValue:
                    value = getValue(featureValueStr);
                    break;
                case functionNameSystem:
                    String systemVersion = "";
                    if (featureValue.containsKey("systemversion")) {
                        systemVersion = featureValue.get("systemversion");
                    }
                    featureColumnName = systemNameMapping(featureName, featureValueStr, systemVersion);
                    value = 1.0;
                    break;
                case functionNameHour:
                    featureColumnName = showTimeMapping(featureName, featureValueStr);
                    value = 1.0;
                    break;
                case functionNameMapping:
                    featureColumnName = featureColumnNameMaping(featureColumnId, featureName, featureValueStr);
                    value = 1.0;
                    break;
                case functionListMapping:
                    featureColumnListMap = featureColumnListMaping(featureColumnId, featureName, featureValueStr);
                    break;
                case functionNameYxStatistics:
                    featureColumnName = yxStatisticsMapping(featureName, featureValueStr);
                    value = 1.0;
                    break;
                case functionNameGetValue2:
                    value = getValue2(featureValueStr,maxStr,minStr);
                    break;
                case functionNameGetDiscretization:
                    if (featureDiscretizationValue == null) {
                        return 1;
                    }
                    featureColumnName = getFeatureDiscretization(featureDiscretizationValue,featureName,featureValueStr);
                    value = 1.0;
                    break;
                case functionNameGetOrigidSta:
                    String origidStr = "";
                    if (featureValue.containsKey("origid")) {
                        origidStr = featureValue.get("origid");
                    }
                    value = getShowClickSta(origidStr, featureValueStr);
                    break;
                case functionNameGetAccountidSta:
                    String accountidStr = "";
                    if (featureValue.containsKey("accountid")) {
                        accountidStr = featureValue.get("accountid");
                    }
                    value = getShowClickSta(accountidStr, featureValueStr);
                    break;
            }

            if(featureProcessingFunName.equals(functionListMapping)){
                for (Map.Entry<String, Double> listMap : featureColumnListMap.entrySet()) {
                    featureColumnValue.put(listMap.getKey(), listMap.getValue());
                    featureKV.put(featureName, listMap.getKey());
                }
            }else {
                featureColumnValue.put(featureColumnName, value);
                featureKV.put(featureName, featureColumnName);
            }
        }
        return 0;
    }

    private static Double getShowClickSta(String idStr, String featureValueStr){
        JSONObject jsonObject = null;
        Double origid_sta = 0.0;
        try {
            jsonObject = JSONObject.parseObject(featureValueStr);
            origid_sta = Double.parseDouble(jsonObject.getString(idStr));
        } catch (Exception e) {
            return 0.0;
        }
        return origid_sta;
    }

    private static Integer getfeatureCrossover(Map<String, String>crossoverFunction, Map<String, String>featureKV, Map<String, Double>featureColumnValue) {
        if (crossoverFunction == null) {
            return 1;
        }
        if(featureKV == null || featureColumnValue == null) {
            return 2;
        }
        for (Map.Entry<String, String> entry : crossoverFunction.entrySet()) {
            String featureColumnNameListStr = entry.getKey();
            String[] featureColumnNameList = featureColumnNameListStr.split(",");
            String featureCrossoverName = "";
            double value = 1.0;
            for (int i = 0; i < featureColumnNameList.length; ++i) {
                String featureName = featureColumnNameList[i];
                String featureColumnName = featureName + "_def";
                if (featureKV.containsKey(featureName)) {
                    featureColumnName = featureKV.get(featureName);
                }
                if (featureCrossoverName.equals("")) {
                    featureCrossoverName = featureColumnName;
                } else {
                    featureCrossoverName = featureCrossoverName + "_" + featureColumnName;
                }
            }
            featureColumnValue.put(featureCrossoverName, value);
        }
        return 0;
    }

    private static Double getUseridOrigidSim(Map<String,String>featureValue) {
        Double value = 0.0;
        if(featureValue == null) {
            return value;
        }
        if(featureValue.containsKey(useridOrigidSim) && featureValue.containsKey(origidKeyName)){
            String [] simKVList = featureValue.get(useridOrigidSim).split(",");
            String origid = featureValue.get(origidKeyName);
            for(int i = 0;i < simKVList.length; ++ i) {
                String [] keyValue = simKVList[i].split(":");
                if(keyValue.length == 2) {
                    String origidTemp = keyValue[0];
                    if(origid.equals(origidTemp)){
                        try {
                            value = Double.parseDouble(keyValue[1]);
                        } catch (NumberFormatException ex) {}
                    }
                }
            }
        }
        return value;
    }

    public static Integer getLeafNode(Integer nowNodeId, TreeNode[] trees, double[] features){
        if(trees == null || features == null){
            return -1;
        }
        if(nowNodeId >= trees.length || nowNodeId < 0){
            return -1;
        }
        if(trees[nowNodeId] == null) {
            return -1;
        }
        if (trees[nowNodeId].getFeaturesId() < -1) {
            return -1;
        }
        if(trees[nowNodeId].getFeaturesId() == -1){
            return trees[nowNodeId].getNodeId();
        }
        Integer id = trees[nowNodeId].getFeaturesId();
        if (id >= features.length){
            return -1;
        }
        Double value = features[trees[nowNodeId].getFeaturesId()];
        if(trees[nowNodeId].compareTo(value) <= 0 && trees[nowNodeId].getLeftId() > nowNodeId){
            return getLeafNode(trees[nowNodeId].getLeftId(), trees, features);
        } else if (trees[nowNodeId].compareTo(value) > 0 && trees[nowNodeId].getRightId() > nowNodeId){
            return getLeafNode(trees[nowNodeId].getRightId(), trees, features);
        } else {
            return -1;
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

    public static Integer getGBDTFeatureColumnValue(Map<String,String>featureValue, Map<String,Double> featureColumnValue){
        if(featureValue == null){
            return 10001;
        }
        if(featureColumnValue == null){
            return 10002;
        }
        Map<String, String>featureColumnIdValue = null;
        Map<String, TreeNode[]> ModelTreeListValue = null;
        Map<String, Map<String, TreeNode[]>> ModelIDTreeListValue = null;
        String groupNameValue = featureValue.get(ModelNameKey);
        if (groupNameValue == null) {
            groupNameValue = "";
        }
        if (featureValue.containsKey(productNameStr)) {
            String valueProductName = featureValue.get(productNameStr);
            if (valueProductName == null) {
                return 10004;
            }
            Integer productType = getProductName(valueProductName);
            if (productType.equals(0)) {
            	if (groupNameValue.equals(ModelImageFeatureNameValue)) {
            	    if (ssjModelConfigNew == null) {
            	        return 10006;
                    }
            		featureColumnIdValue = ssjModelConfigNew.get(featureNameIdKeyStr + "_" + ssjConfigNewSuff);
            		ModelTreeListValue = ssjModelTreeListNew;
            	} else {
            	    featureColumnIdValue = featureColumnIdSsj;
            	    ModelTreeListValue = ssjModelTreeList;
                }
            } else if (productType.equals(1)) {
                if (groupNameValue.equals(ModelImageFeatureNameValue)) {
                    String positionIdStr = "";
                    if (featureValue.containsKey(positionidStr)) {
                        if (featureValue.get(positionidStr) != null) {
                            positionIdStr = featureValue.get(positionidStr);
                        }
                    }
                    if (positionIdStr.equals("SP")) {
                        if (knSpModelConfig == null) {
                            return 10007;
                        }
                        featureColumnIdValue = knSpModelConfig.get(featureNameIdKeyStr + "_" + knSpConfigSuff);
                        ModelTreeListValue = knSpModelTreeList;
                    } else {
                        featureColumnIdValue = featureColumnIdKnGbdtNew;
                        ModelTreeListValue = knModelTreeListNew;
                        ModelIDTreeListValue = knModelIDTreeListNew;
                    }

                } else {
                    featureValue.put(testModelNameKey, testModelNameValue);
                    featureColumnIdValue = featureColumnIdKnGbdt;
                    ModelTreeListValue = knModelTreeList;
                }
            } else {
                return 10005;
            }
        } else {
            return 10003;
        }

        Integer ret = getFeatureColumnValue(featureValue, featureColumnValue);
        if (ret != 0) {
            return 10000+ret;
        }
        int featureColumnIdKnGbdtSize = featureColumnIdValue.size() + 1;
        double[] features = new double[featureColumnIdKnGbdtSize];
        if (ModelTreeListValue != null || ModelIDTreeListValue != null) {
            getFeaturesList(featureColumnIdValue, featureColumnValue, features);
        }

        Integer treeRet = getTreeFeature(features, ModelTreeListValue, featureColumnValue);
        if (treeRet != 0) {
            return 0;
        }
        getIDTreeFeature(ModelIDTreeListValue, featureValue, features, featureColumnValue);

        return 0;
    }

    private static Integer getFeaturesList(Map<String, String>featureColumnIdValue, Map<String, Double>featureColumnValue, double[] features) {
        int featureColumnIdGbdtSize = featureColumnIdValue.size() + 1;
        for (Map.Entry<String, String> entry : featureColumnIdValue.entrySet()) {
            if (featureColumnValue.containsKey(entry.getKey()) && isInteger(entry.getValue())) {
                int posId = Integer.parseInt(entry.getValue()) - 1;
                if (posId < featureColumnIdGbdtSize && posId >= 0) {
                    features[posId] = featureColumnValue.get(entry.getKey());
                }
            }
        }
        return 0;
    }

    private static Integer getTreeFeature(double[] features, Map<String, TreeNode[]> ModelTreeListValue, Map<String,Double> featureColumnValue) {
        if (ModelTreeListValue == null || features == null || featureColumnValue == null) {
            return 1;
        }
        for (Map.Entry<String, TreeNode[]> entry : ModelTreeListValue.entrySet()) {
            Integer leafNodeId = getLeafNode(1, entry.getValue(), features);
            String featureKey = ModelNameIdSuff + entry.getKey() + "_" + leafNodeId;
            featureColumnValue.put(featureKey, 1.0);
        }
        return 0;
    }

    private static Integer getIDTreeFeature(Map<String, Map<String, TreeNode[]>> ModelIDTreeListValue, Map<String, String> featureValue, double[] features, Map<String, Double>featureColumnValue){
        if (ModelIDTreeListValue == null) {
            return 1;
        }
        String origidValue = "def";
        if(featureValue.containsKey(origidKeyName)){
            origidValue = featureValue.get(origidKeyName);
        }
        if(ModelIDTreeListValue.containsKey(origidValue)){
            for (Map.Entry<String, TreeNode[]> entry : ModelIDTreeListValue.get(origidValue).entrySet()) {
                Integer leafNodeId = getLeafNode(1, entry.getValue(), features);
                String featureKey = knModelOrigIdTreeNameIdSuff + origidValue + "_" + entry.getKey() + "_" + leafNodeId;
                featureColumnValue.put(featureKey, 1.0);
            }
        }
        return 0;
    }

    private static String featureColumnNameMaping(Map<String, String> featureColumnId, String featureName, String featureValueStr){
        String key = featureName + "_" + featureValueStr;
        if(featureColumnId == null){
            return featureName + "_def";
        }
        if(featureColumnId.containsKey(key)){
            return key;
        } else{
            return featureName + "_def";
        }
    }

    private static Map<String, Double> featureColumnListMaping(Map<String, String> featureColumnId, String featureName, String featureValueStr){
        Map<String, Double> mapResult = new HashMap<String, Double>();
        double value = 1.0;
        String key = "";
        String reg = "";

        if(featureColumnId == null){
            key = featureName + "_def";
            mapResult.put(key, value);
            return mapResult;
        }

        if (featureValueStr.contains("#")){
            reg = "#";
        } else if (featureValueStr.contains(",")){
            reg = ",";
        } else if (featureValueStr.contains("\\|")){
            reg = "\\|";
        } else{
            reg = " ";
        }

        String[] listStr = featureValueStr.split(reg);

        for(int i = 0; i < listStr.length; i++){
            if(featureColumnId.containsKey(featureName + "_" + listStr[i])){
                key = featureName + "_" + listStr[i];
            }else {
                key = featureName + "_def";
            }

            if(mapResult.containsKey(key)){
                value = mapResult.get(key) + 1.0;
                mapResult.put(key, value);
            }else {
                mapResult.put(key, 1.0);
            }
        }
        return mapResult;
    }

    private static boolean isInteger(String str) {
        if(str == null){
            return false;
        }
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]+$");
        return pattern.matcher(str).matches();
    }

    private static  String yxStatisticsMapping(String featureName,String countStr){
        if(countStr == null){
            return featureName + "_def";
        }
        String value = "_def";
        Integer countInt = 0;
        if(isInteger(countStr) == true) {
            try {
                countInt = Integer.parseInt(countStr);
                if(countInt >= 10){
                    value = "_10";
                }else if(countInt < 0){
                    value = "_def";
                }else{
                    value = "_" + countStr;
                }
            } catch (NumberFormatException e) {
            }
        }
        return featureName + value;
    }

    private static Double getValue(String keyStr){
        try {
            Double value = Double.parseDouble(keyStr);
            if(value < 0){
                value = 0.0;
            }
            return value;
        } catch (NumberFormatException e){
            return 0.0;
        }
    }

    private static  String showTimeMapping(String featureName,String timeStr){
        /*try{
            SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
            Date dt = ft.parse(timeStr);
            System.out.println(dt);
            return String.format(Locale.US,"%ta",dt);
        } catch (ParseException e) {
            return "def";
        }*/
        Pattern pattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2} (\\d{2}):");
        Matcher matcher = pattern.matcher(timeStr);
        Integer hour = 0;
        if (matcher.find()) {
            try{
                hour = Integer.parseInt(matcher.group(1)) + 1;
            } catch (NumberFormatException e){
                //System.out.println(e);
            }
        }
        return featureName + "_" + Integer.toString(hour);
    }

    private static String systemNameMapping(String featureName,String system,String systemVersion){
        Integer systemId = 0;
        String systemLower = system.toLowerCase();
        if(systemLower.equals("android")){
            systemId = 1;
        } else if(systemLower.equals("ios")){
            systemId = 2;
        }
        Integer systemVersionId = 0;
        String[] systemVersionStrList = systemVersion.split("\\.");
        if(systemVersionStrList.length > 0) {
            try {
                systemVersionId = Integer.parseInt(systemVersionStrList[0].toString());
                if (systemVersionId > 14 || systemVersionId < 1) {
                    systemVersionId = 0;
                }
            } catch (NumberFormatException e) {
                //System.out.println(e);
            }
        }
        return  featureName + "_" + Integer.toString(systemId * 15 + systemVersionId);
    }

    private static Double getValue2(String keyStr, String maxStr, String minStr) {
        Double value;
        Double maxValue;
        Double minValue;
        try {
            value = Double.parseDouble(keyStr);
            maxValue = Double.parseDouble(maxStr);
            minValue = Double.parseDouble(minStr);
            if (value<minValue) {
                return 0.0;
            } else if (value>maxValue) {
                return 1.0;
            }
            return (value-minValue)/(maxValue-minValue);
        } catch (Exception e){
            return 0.0;
        }
    }

    private static String getFeatureDiscretization(Map<String,String> featureDiscretization,String featureName,String featureValueStr) {
        if (featureDiscretization == null) {
            return "error";
        }
        Pattern pattern1 = Pattern.compile("^[\\+-]?[\\d]*$");
        Pattern pattern2 = Pattern.compile("[+-]{0,1}\\d+\\.\\d*|[+]{0,1}\\d*\\.\\d+");
        if (featureValueStr.equals("NULL") ||  featureValueStr.equals("")) {
            return featureName + "_def";
        }
        if (!pattern1.matcher(featureValueStr).matches() && !pattern2.matcher(featureValueStr).matches()) {
            return featureName + "_def";
        }
        if (featureDiscretization.containsKey(featureName)) {
            if (featureDiscretization.get(featureName) == null || featureDiscretization.get(featureName).equals("")) {
                return featureName + "_def";
            }
            String[] splitListStr = featureDiscretization.get(featureName).split(",");

            for (int i=0; i<splitListStr.length; i++) {
                if (!pattern1.matcher(splitListStr[i]).matches() && !pattern2.matcher(splitListStr[i]).matches()) {
                    return featureName + "_def";
                }
                if (Double.parseDouble(featureValueStr) < Double.parseDouble(splitListStr[i])) {
                    return featureName + "_" + splitListStr[i];
                }
            }
            return featureName + "_" + splitListStr[splitListStr.length-1] + "+";
        }
        return featureName + "_def";
    }
}
