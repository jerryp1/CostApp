package com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphVertex;

import com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphJobDomain;
import com.aliyun.odps.graph.ComputeContext;
import com.aliyun.odps.graph.Edge;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.io.*;

import java.io.IOException;
import java.util.HashMap;

public class MyGraphVertex extends Vertex<Text, MapWritable, MapWritable, MapWritable> {
    private static final GraphJobDomain gd = new GraphJobDomain();

    private boolean isStartVertex() {
        return ((BooleanWritable)this.getValue().get(gd.IF_START)).get();
    }

    private void clearVertexCost() {
        this.getValue().put(gd.NODE_COST, new MapWritable());
    }

    private double getVertexDescEdgeTotalWeight(){
        double totalEdgeWeight = 0.0;
        for(Edge<Text, MapWritable> edge: this.getEdges()){
            totalEdgeWeight = totalEdgeWeight + Double.parseDouble( edge.getValue().get(gd.EDGE_WEIGHT).toString() );
        }
        return totalEdgeWeight;
    }


    private MapWritable getVertexApportionCostByEdgeRation(double edgeRation){
        MapWritable nodeCostMap = (MapWritable) this.getValue().get(gd.NODE_COST);
        MapWritable newNodeCost = new MapWritable();

        for(Writable nodeId: nodeCostMap.keySet()){
            double nodeIdRation = ((DoubleWritable) nodeCostMap.get(nodeId)).get() * edgeRation;
            newNodeCost.put(nodeId, new DoubleWritable(nodeIdRation) );
        }
        return newNodeCost;
    }

    private void costApportionForSingleVertex(ComputeContext<Text, MapWritable, MapWritable, MapWritable> context) throws IOException {

        double totalEdgeWeight = getVertexDescEdgeTotalWeight();
        for(Edge<Text, MapWritable> edge: this.getEdges()){
            MapWritable msgBody = new MapWritable();

            double edgeRation =  Double.parseDouble( edge.getValue().get(gd.EDGE_WEIGHT).toString() ) / totalEdgeWeight;

            MapWritable nodeApportionCost = getVertexApportionCostByEdgeRation(edgeRation);
            if(nodeApportionCost.keySet().size() > 0) {
                MapWritable sendMsgNodeList = new MapWritable();
                sendMsgNodeList.put(this.getId(), gd.TEXT_EMPTY);
                msgBody.put(gd.NODE_ANCESTOR_APPORTION_MAP, nodeApportionCost);
                msgBody.put(gd.MSG_NODE_LIST, sendMsgNodeList);
                context.sendMessage(edge.getDestVertexId(), msgBody);
            }
        }
        clearVertexCost();
    }

    private void insertIntoNodeCost(MapWritable apportionedParentNodeCost){
        MapWritable nodeCostMap = (MapWritable) this.getValue().get(gd.NODE_COST);
        if(apportionedParentNodeCost.size() > 0) {
            for (Writable ancestorNodeId : apportionedParentNodeCost.keySet()) {

                if (nodeCostMap.containsKey(ancestorNodeId)) {
                    double ancestorNodeRation = ((DoubleWritable)apportionedParentNodeCost.get(ancestorNodeId)).get();
                    double thisNodeRation = ((DoubleWritable)nodeCostMap.get(ancestorNodeId)).get();
                    nodeCostMap.put(ancestorNodeId, new DoubleWritable(ancestorNodeRation + thisNodeRation));
                } else {
                    nodeCostMap.put(ancestorNodeId, apportionedParentNodeCost.get(ancestorNodeId));
                }
            }
        }
    }

    private void updateCostAndVisitedListFromMsg(Iterable<MapWritable> messages) {
        MapWritable nodeVisitedNodes = (MapWritable) this.getValue().get(gd.NODE_VISITED_VERTEX);
        for (MapWritable msg : messages) {
            //每个msg含有两个key，NODE_ANCESTOR_APPORTION_MAP 和 MSG_NODE_LIST
            if(msg.containsKey(gd.MSG_NODE_LIST)){
                MapWritable nodeListMap = (MapWritable) msg.get(gd.MSG_NODE_LIST);
                for(Writable nodeId: nodeListMap.keySet()){
                    if(!nodeVisitedNodes.containsKey(nodeId)){
                        nodeVisitedNodes.put(nodeId, gd.TEXT_EMPTY);
                    }
                }
            }
            if(msg.containsKey(gd.NODE_ANCESTOR_APPORTION_MAP)){
                MapWritable apportionMap = (MapWritable) msg.get(gd.NODE_ANCESTOR_APPORTION_MAP);
                insertIntoNodeCost(apportionMap);
            }
        }
    }

    private void setParentCnt(Iterable<MapWritable> messages){
        HashMap<String,String> nodeList = new HashMap<>();
        if(messages.iterator().hasNext()){
            for(MapWritable msg: messages){
                if(msg.containsKey(gd.MSG_NODE_LIST)){
                    MapWritable nodeListMap = (MapWritable) msg.get(gd.MSG_NODE_LIST);
                    for(Writable nodeId: nodeListMap.keySet()){
                        if(! nodeList.containsKey(nodeId.toString())){
                            nodeList.put(nodeId.toString(), "");
                        }
                    }
                }
            }
        }
        this.getValue().put(gd.NODE_PARENT_CNT, new LongWritable(nodeList.keySet().size()));
    }

    private boolean canApportion(){
        return ((LongWritable) this.getValue().get(gd.NODE_PARENT_CNT)).get() == ((MapWritable)this.getValue().get(gd.NODE_VISITED_VERTEX)).keySet().size();
    }

    @Override
    public void compute(ComputeContext<Text, MapWritable, MapWritable, MapWritable> context,
        Iterable<MapWritable> messages) throws IOException {
        // init message
        if (context.getSuperstep() == 0L) {
            //for the first step, msg is sent for vertex's children counting its parents.
            this.getValue().put(gd.NODE_VISITED_VERTEX,  new MapWritable());
            if (this.hasEdges()) {
                for (Edge<Text, MapWritable> edge : this.getEdges()) {
                    MapWritable msgToSend = new MapWritable();

                    MapWritable costMap = new MapWritable();
                    MapWritable nodeList = new MapWritable();
                    nodeList.put(this.getId(), gd.TEXT_EMPTY);
                    msgToSend.put(gd.MSG_NODE_LIST, nodeList);
                    msgToSend.put(gd.NODE_ANCESTOR_APPORTION_MAP, costMap);
                    context.sendMessage(edge.getDestVertexId(), msgToSend);
                }
            }
        }
        else if (context.getSuperstep() == 1L) {
            setParentCnt(messages);
            // every node will execute one of the branches below.
            if (this.hasEdges()) {
                if (!messages.iterator().hasNext()) {
                    //this node has at least one child, and since the absence of message, it is a 【【root】】 in the network.
                    //one may prefer the node writes down its own cost.
                    costApportionForSingleVertex(context);
                    //when node arrives here, the costs of it will be zero. Moreover, the costs are sent to its children uniformly.
                }
            }
            else {
                //the node must be a leaf node, namely, has no child.
                MapWritable nodeCostMap = (MapWritable) this.getValue().get(gd.NODE_COST);
                for(Writable nodeId: nodeCostMap.keySet()){
                    context.write(this.getId(), nodeId, new Text(nodeCostMap.get(nodeId).toString()));
                }
                clearVertexCost();
            }
            voteToHalt();
        } else if (messages.iterator().hasNext()) {
            updateCostAndVisitedListFromMsg(messages);

            if(Boolean.parseBoolean(this.getValue().get(gd.IF_LEAF).toString()) || !this.hasEdges()){
                MapWritable nodeCostMap = (MapWritable)this.getValue().get(gd.NODE_COST);
                for (Writable nodeId : nodeCostMap.keySet()) {
                    context.write(this.getId(), nodeId, new Text(nodeCostMap.get(nodeId).toString()));
                }
                clearVertexCost();
            }
            else if (this.hasEdges() && canApportion()) {
                costApportionForSingleVertex(context);
            }
            voteToHalt();
        }
    }

}
