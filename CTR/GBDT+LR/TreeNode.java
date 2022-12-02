package com.feidee.bigdata.online.ad.feature;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Created by Administrator on 2017/7/4.
 */
public class TreeNode {
    private Integer nodeId;
    private Integer leftId;
    private Integer rightId;
    private Double threshold;
    private Integer featuresId;

    public TreeNode(Integer nodeId, Integer leftId, Integer rightId, Double threshold, Integer featuresId) {
        this.nodeId = nodeId;
        this.leftId = leftId;
        this.rightId = rightId;
        this.threshold = threshold;
        this.featuresId = featuresId;
    }

    public TreeNode(String valueString){
        String [] valueSplit = valueString.split(",");
        if(valueSplit.length == 5) {
            if (isNum(valueSplit[0])) {
                this.nodeId = Integer.parseInt(valueSplit[0]);
            } else {
                this.nodeId = -1;
            }

            if (isNum(valueSplit[1])) {
                this.leftId = Integer.parseInt(valueSplit[1]);
            } else {
                this.leftId = -1;
            }

            if (isNum(valueSplit[2])) {
                this.rightId = Integer.parseInt(valueSplit[2]);
            } else {
                this.rightId = -1;
            }

            if (isDouble(valueSplit[3])) {
                this.threshold = Double.parseDouble(valueSplit[3]);
            } else {
                this.threshold = -1.0;
            }

            if (isNum(valueSplit[4])) {
                this.featuresId = Integer.parseInt(valueSplit[4]);
            } else {
                this.featuresId = -1;
            }
        } else {
        	this.nodeId = -1;
            this.leftId = -1;
            this.rightId = -1;
            this.threshold = -1.0;
            this.featuresId = -1;
        }
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public Integer getLeftId() {
        return leftId;
    }

    public Integer getRightId() {
        return rightId;
    }

    public Double getThreshold() {
        return threshold;
    }

    public Integer getFeaturesId() {
        return featuresId;
    }

    public Integer compareTo(Double featureValue) {
        return featureValue.compareTo(threshold);
    }

    @Override
    public String toString() {
        return nodeId +
                "," + leftId +
                "," + rightId +
                "," + threshold +
                ',' + featuresId;
    }


    private static boolean isNum(String str) {
        try {
            Integer.parseInt(str) ;
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isDouble(String str) {
        try {
            Double.parseDouble(str) ;
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
