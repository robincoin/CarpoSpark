package com.carpo.spark.bean;

import org.apache.commons.collections.map.HashedMap;

import java.util.Map;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public class DAGBean {
    private CarpoNodes nodes;//节点
    private String nodeId;//节点ID
    private Map<String, DAGBean> dagMap;//对应的子节点信息

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public CarpoNodes getNodes() {
        return nodes;
    }

    public void setNodes(CarpoNodes nodes) {
        this.nodes = nodes;
    }

    public Map<String, DAGBean> getDagMap() {
        if (this.dagMap == null) this.dagMap = new HashedMap();
        return dagMap;
    }

    public void put(String id, DAGBean dag) {
        getDagMap().put(id, dag);
        nodes.setChilds(nodes.getChilds() + 1);
    }

    public void setDagMap(Map<String, DAGBean> dagMap) {
        this.dagMap = dagMap;
    }
}
