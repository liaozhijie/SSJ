package com.feidee.bigdata.online.ad.feature;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/**
 * Created by Administrator on 2017/6/28.
 */
public class SparkGBDTmodel extends FeatureProcessingHelp {
    public static List<String> createGBDTModel(JavaRDD<LabeledPoint> trainDataGbdt, JavaRDD<LabeledPoint> testDataGbdt, String suff, int numTrees, int maxTreeDepth) throws IOException {
        BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
        boostingStrategy.setNumIterations(numTrees);
        boostingStrategy.getTreeStrategy().setNumClasses(2);
        boostingStrategy.getTreeStrategy().setMaxDepth(maxTreeDepth);
        //子节点拥有的最小样本示例
        //boostingStrategy.getTreeStrategy().setMinInstancesPerNode(1000);
        //最小的信息增益值
        boostingStrategy.getTreeStrategy().setMinInfoGain(0.0);
        //训练数据的抽样率
        boostingStrategy.getTreeStrategy().setSubsamplingRate(1.0);

        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

        final GradientBoostedTreesModel gbdtModel = GradientBoostedTrees.train(trainDataGbdt, boostingStrategy);
        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = testDataGbdt.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = gbdtModel.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );
        JavaRDD<Tuple2<Object, Object>> predictionAndLabelsTrain = trainDataGbdt.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = gbdtModel.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );

        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictionAndLabels.rdd());
        BinaryClassificationMetrics metricsTrain = new BinaryClassificationMetrics(predictionAndLabelsTrain.rdd());
        double auroc = metrics.areaUnderROC();
        double auprc = metrics.areaUnderPR();
        double trainAUC = metricsTrain.areaUnderROC();
        double trainAP =  metricsTrain.areaUnderPR();
        List<String> ans = new ArrayList<>();
        List<String> treeIdName = new ArrayList<>();
        ans.add("test-auc:"+String.valueOf(auroc)+" auprc:"+String.valueOf(auprc));
        ans.add("train-auc:"+String.valueOf(trainAUC)+" auprc:"+String.valueOf(trainAP));
        for(int i = 0;i < gbdtModel.numTrees(); ++ i) {
            String treesStr = suff + "\t" + i + "\t" + getAllNode(gbdtModel.trees()[i].topNode(), String.valueOf(i), treeIdName);
            ans.add(treesStr);
        }
        return ans;
    }

    public static String[] getOneHotRowName(Map<String, Map<String, String>>config) {
        Map<String, String> featureNameIdTemp = new ConcurrentHashMap<>();
        String modelOneHotKey = "";
        for (String configKey : config.keySet() ) {
            if (configKey.contains("feature_name_id")) {
                modelOneHotKey = configKey;
                break;
            }
        }
        featureNameIdTemp = config.get(modelOneHotKey);
        Integer featureColumnLen = featureNameIdTemp.size();
        final String[] oneHotRowName = new String[featureColumnLen + 1];
        for(Map.Entry<String, String> entry : featureNameIdTemp.entrySet()){
            oneHotRowName[Integer.parseInt(entry.getValue())] = entry.getKey();
        }
        return oneHotRowName;
    }

    public static void saveLrResultValue(JavaSparkContext sc, final LogisticRegressionModel model, JavaRDD<LabeledPoint> trainDataGbdt, JavaRDD<LabeledPoint> testDataGbdt, String savePath) {
        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = testDataGbdt.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );
        JavaRDD<Tuple2<Object, Object>> predictionAndLabels2 = trainDataGbdt.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictionAndLabels.rdd());
        BinaryClassificationMetrics metrics2 = new BinaryClassificationMetrics(predictionAndLabels2.rdd());
        double auroc = metrics.areaUnderROC();
        double auprc = metrics.areaUnderPR();
        double auroc2 = metrics2.areaUnderROC();
        double auprc2 = metrics2.areaUnderPR();
        String str = "AUROC:" + auroc + "   auprc:" + auprc;
        String str2 = "AUROC:" + auroc2 + "   auprc:" + auprc2;

        String model_str = model.weights().toString();
        List<String> strList = new ArrayList<String>();
        strList.add(str);
        strList.add(str2);
        strList.add(model_str);
        JavaRDD<String>rddList = sc.parallelize(strList);
        rddList.saveAsTextFile(savePath);
    }

    public static void main(String[] args) throws IOException {
        final Map<String, Map<String, String>> modelConfig = new HashMap<>();
        final Map<String, String> runConfig = new HashMap<>();
        String modelBranch = "gbdt";
        if (args.length != 3) {
            System.out.println("参数输入错误！");
            return;
        } else {
            String modelConfigFile = args[0];
            String runConfigFile = args[1];
            modelBranch = args[2];
            loadDisorderedValue(modelConfigFile, modelConfig);
            loadRunConfig(runConfigFile,runConfig);
        }
        String tableName = runConfig.get("table_name");
        Integer filterGBDTProportion =  Integer.parseInt(runConfig.get("gbdt_proportion"));
        Integer filterLRProportion =  Integer.parseInt(runConfig.get("lr_proportion"));
        Integer testDay = Integer.parseInt(runConfig.get("test_days"));
        final String modelLine = runConfig.get("model_line");
        String gbdtModelSavePath = runConfig.get("gbdt_model_save_path");
        String lrModelSavePath = runConfig.get("lr_model_save_path");
        final String featureNameIdStr = runConfig.get("feature_name_id_str");

        int numTrees = 100;
        int maxTreeDepth = 3;
        if (runConfig.containsKey("numTrees")) {
            numTrees = Integer.parseInt(runConfig.get("numTrees"));
        }
        if (runConfig.containsKey("maxTreeDepth")) {
            maxTreeDepth = Integer.parseInt(runConfig.get("maxTreeDepth"));
        }

        SparkConf conf = new SparkConf();
        conf.set("master", "yarn-client");
        conf.set("driver-memory", "3G");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hc = new HiveContext(sc);
        DataFrame df = hc.table(tableName);
        final String[] finalTableList = df.columns();

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        cal.add(Calendar.DATE, -testDay);
        String split_day = sdf.format(cal.getTime());

        DataFrame testDF = df.filter(df.col("train_ymd").geq(split_day));
        DataFrame trainDF = df.filter(df.col("train_ymd").lt(split_day));
        final String[] oneHotRowName = getOneHotRowName(modelConfig);
        int negSampleCount = (int)trainDF.where("clicktime is null").count();
        int SampleCount = (int)trainDF.count();
        System.out.println("=====filterGBDTProportion:" + filterGBDTProportion + "=====SampleCount:" +  SampleCount + "======negSampleCount:" +negSampleCount);

        if(modelBranch.equals("gbdt")) {
            final Integer filterNum = (negSampleCount / (filterGBDTProportion*(SampleCount - negSampleCount)));
            final Map<String, String>filter = new ConcurrentHashMap<>();
            String pro = "gbdt";
            JavaRDD<LabeledPoint> trainData = trainDF.javaRDD().map(new Function<Row, String>() {
                @Override
                public String call(Row row) throws Exception {
                    return featureSparkModelRow(modelConfig, finalTableList, oneHotRowName, row, filterNum, "gbdt", filter, modelLine);
                }
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    return !s.equals("");
                }
            }).map(new Function<String, LabeledPoint>() {
                @Override
                public LabeledPoint call(String s) throws Exception {
                    return getLabeledPoint(s);
                }
            });

            JavaRDD<LabeledPoint> testData = testDF.javaRDD().map(new Function<Row, String>() {
                @Override
                public String call(Row row) throws Exception {
                    return featureSparkModelRow(modelConfig, finalTableList, oneHotRowName, row, 1, "gbdt", filter, modelLine);
                }
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    return !s.equals("");
                }
            }).map(new Function<String, LabeledPoint>() {
                @Override
                public LabeledPoint call(String s) throws Exception {
                    return getLabeledPoint(s);
                }
            });

            List<String> strList = createGBDTModel(trainData.cache(), testData, knModelTreesStr, numTrees, maxTreeDepth);

            JavaRDD<String> rddList = sc.parallelize(strList);
            rddList.saveAsTextFile(gbdtModelSavePath);
        } else if(modelBranch.equals("gbdt+lr")) {
            final Integer filterNum = (negSampleCount / (filterLRProportion*(SampleCount - negSampleCount)));
            final Map<String, String>filter = new ConcurrentHashMap<>();
            JavaRDD<LabeledPoint> trainData = trainDF.javaRDD().map(new Function<Row, LabeledPoint>() {
                @Override
                public LabeledPoint call(Row row) throws Exception {
                    return featureSparkModelRowNew(modelConfig, finalTableList, row, filterNum, "gbdt+lr", filter, modelLine, featureNameIdStr);
                }
            }).filter(new Function<LabeledPoint, Boolean>() {
                @Override
                public Boolean call(LabeledPoint s) throws Exception {
                    return s.label() != -1.0;
                }
            });

            JavaRDD<LabeledPoint> testData = testDF.javaRDD().map(new Function<Row, LabeledPoint>() {
                @Override
                public LabeledPoint call(Row row) throws Exception {
                    return featureSparkModelRowNew(modelConfig, finalTableList, row, 1, "gbdt+lr", filter, modelLine, featureNameIdStr);
                }
            }).filter(new Function<LabeledPoint, Boolean>() {
                @Override
                public Boolean call(LabeledPoint s) throws Exception {
                    return s.label() != -1.0;
                }
            });

            JavaRDD<LabeledPoint> trainDataGbdt = trainData.cache();
            JavaRDD<LabeledPoint> testDataGbdt = testData;

            final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainDataGbdt.rdd());
            model.clearThreshold();
            saveLrResultValue(sc,model,trainDataGbdt,testDataGbdt,lrModelSavePath);
        }

    }

}
