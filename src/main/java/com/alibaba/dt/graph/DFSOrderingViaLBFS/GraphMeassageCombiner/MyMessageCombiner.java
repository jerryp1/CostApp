package com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphMeassageCombiner;

import com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphJobDomain;
import com.aliyun.odps.graph.Combiner;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.MapWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;

import java.io.IOException;

public class MyMessageCombiner extends Combiner<Text, MapWritable> {
    private static final GraphJobDomain gd = new GraphJobDomain();

    @Override
    public void combine(Text vertexId, MapWritable combinedMessage, MapWritable messageToCombine) throws IOException {
        for (Writable nodeId : messageToCombine.keySet()) {
            MapWritable msgBody = (MapWritable)messageToCombine.get(nodeId);
            if (combinedMessage.containsKey(nodeId)) {
                if(!msgBody.containsKey(gd.TEXT_TEST)) {
                    MapWritable combinedMsgBody = (MapWritable)combinedMessage.get(nodeId);
                    double combinedTableSize = Double.parseDouble(combinedMsgBody.get(gd.MSG_TABLE_SIZE).toString());
                    double combinedCpuCost = Double.parseDouble(combinedMsgBody.get(gd.MSG_CPU_COST).toString());
                    double combinedApportionCost = Double.parseDouble(
                        combinedMsgBody.get(gd.MSG_APPORTION_COST).toString());

                    double msgTableSize = Double.parseDouble(msgBody.get(gd.MSG_TABLE_SIZE).toString());
                    double msgCpuCost = Double.parseDouble(msgBody.get(gd.MSG_CPU_COST).toString());
                    double msgApportionCost = Double.parseDouble(msgBody.get(gd.MSG_APPORTION_COST).toString());

                    MapWritable newMsgBody = new MapWritable();
                    newMsgBody.put(gd.MSG_TABLE_SIZE, new DoubleWritable(combinedTableSize + msgTableSize));
                    newMsgBody.put(gd.MSG_CPU_COST, new DoubleWritable(combinedCpuCost + msgCpuCost));
                    newMsgBody.put(gd.MSG_APPORTION_COST, new DoubleWritable(combinedApportionCost + msgApportionCost));
                    combinedMessage.put(nodeId, newMsgBody);
                }
            } else {
                combinedMessage.put(nodeId, messageToCombine.get(nodeId));
            }

        }

    }
}
