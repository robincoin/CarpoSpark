package com.carpo.spark.bean;

import com.carpo.spark.utils.StringsUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * 根据Json，获取执行流程图，
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/5
 */
public class CarpoFlowChart {
    private CarpoTask carpoTask;
    private DAGBean dagBean = new DAGBean();//终点

    public CarpoFlowChart(CarpoTask carpoTask) {
        this.carpoTask = carpoTask;
        this.analysis();
    }

    public DAGBean getDagBean() {
        return dagBean;
    }

    /**
     * 是否有效的DAG
     *
     * @return
     */
    public boolean isValid() {
        if (dagBean.getDagMap().isEmpty()) return false;
        return true;
    }

    /**
     * 进行DAG分析
     */
    private void analysis() {
        Map<String, CarpoLines> lineMap = carpoTask.getLines();
        Map<String, CarpoNodes> nodeMap = carpoTask.getNodes();
        if (lineMap == null) return;
        Set<String> lines = new HashSet<>();
        //检查连接线是否有孤立的
        for (String l : lines) {
            CarpoLines line = lineMap.get(l);
            if (StringsUtils.isEmpty(line.getInputs()) || StringsUtils.isEmpty(line.getOutputs()) || line.getInputs().equals(line.getOutputs())) {
                lineMap.remove(l);
            }
        }

        //查找终点
        for (final String nodeId : nodeMap.keySet()) {
            CarpoNodes node = nodeMap.get(nodeId);
            node.setId(nodeId);
            if (node != null && ENodeType.output.name().equals(node.getType())) {
                dagBean.setNodeId(nodeId);
                dagBean.setNodes(node);
            }
        }

        //反方向向上查找构成有向无环图
        findParentNode(dagBean);
    }

    //查找父节点
    private void findParentNode(DAGBean pdag) {
        for (String lineId : carpoTask.getLines().keySet()) {
            CarpoLines line = carpoTask.getLines().get(lineId);
            if (line.getOutputs().contains(pdag.getNodeId())) {
                for (final String sId : line.getInputs().split(",")) {
                    CarpoNodes node = carpoTask.getNodes().get(sId);
                    if (node != null) {
                        DAGBean dag = new DAGBean();
                        dag.setNodeId(sId);
                        dag.setNodes(node);
                        pdag.put(sId, dag);
                        findParentNode(dag);
                    }
                }
            }
        }
    }

    /**
     * 获取节点的执行顺序
     * @return
     */
    public Stack<CarpoNodes> getExeOrder() {
        Stack<CarpoNodes> stack = new Stack<>();
        putStack(stack, dagBean, dagBean.getNodeId());
        return stack;
    }

    private void putStack(Stack<CarpoNodes> stack, DAGBean dagBean, String id) {
        stack.push(dagBean.getNodes().setRefId(id));
        for (DAGBean dag : dagBean.getDagMap().values()) {
            putStack(stack, dag, dagBean.getNodeId());
        }
    }

}
