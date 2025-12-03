package com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphVertex;
import com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphJobDomain;
import com.aliyun.odps.graph.ComputeContext;
import com.aliyun.odps.graph.Edge;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.io.MapWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;

import java.io.IOException;

public class MyGraphVertex extends Vertex<Text, MapWritable, MapWritable, MapWritable> {
    private static final GraphJobDomain gd = new GraphJobDomain();

    private boolean lexicalLarger(String A, String B){
        String[] AList = A.split(",");
        String[] BList = B.split(",");
        if(AList.length > BList.length ){
            for(int i = 0; i < AList.length; i++){
                long x  = Long.parseLong(AList[i]);
                long y = 0;
                if(i < BList.length){
                    if("null".equals(BList[i])){
                        y = Long.MAX_VALUE;
                    }else {
                        y = Long.parseLong(BList[i]);
                    }
                }
                if(x < y){
                    return true;
                } else if (x > y){
                    return false;
                }
            }
        }else {
            for(int i = 0; i < BList.length; i++){
                long y;

                if("null".equals(BList[i])){
                    y = Long.MAX_VALUE;
                }else {
                    y = Long.parseLong(BList[i]);
                }

                long x = 0;
                if(i < AList.length){
                    x = Long.parseLong(AList[i]);
                }
                if(x < y){
                    return true;
                } else if (x > y){
                    return false;
                }
            }
        }
        return false;
    }

    private boolean setLocalVertexLabel(Text labelOrdering){
        boolean ifUpdate = false;
        String newLabel = labelOrdering.toString();
        String vertexLabel = this.getValue().get(gd.NODE_LABEL).toString();
        if(lexicalLarger(newLabel, vertexLabel)){
            this.getValue().put(gd.NODE_LABEL, new Text(newLabel));
            ifUpdate = true;
        }
        return ifUpdate;
    }

    @Override
    public void cleanup(WorkerContext<Text, MapWritable, MapWritable, MapWritable> context) throws IOException {
        if(!"START_VIRTUAL_NODE".equals(this.getId().toString())) {
            context.write(this.getId(), this.getValue().get(gd.NODE_LABEL));
        }
    }
    
    @Override
    public void compute(ComputeContext<Text, MapWritable, MapWritable, MapWritable> context,
        Iterable<MapWritable> messages) throws IOException {
        // init message
        if (context.getSuperstep() == 0L) {
            //从所有的source开始，出发寻找sink
            if("START_VIRTUAL_NODE".equals(this.getId().toString()) && this.hasEdges()){
                long index = 1;
                for(Edge<Text, MapWritable> edge: this.getEdges()){
                    MapWritable msgBody = new MapWritable();
                    msgBody.put(this.getId(), new Text(String.valueOf(index)));
                    context.sendMessage(edge.getDestVertexId(), msgBody);
                    index++;
                }
            }
        }
        else if (messages.iterator().hasNext()) {
            boolean ifUpdate = false;
            for(MapWritable msg: messages){
                for(Writable nodeId: msg.keySet()){
                    Text labelOrdering = (Text) msg.get(nodeId);
                    //System.out.println("super step ["+context.getSuperstep()+"], vertex_"+this.getId()+" receive label <"+labelOrdering.toString()+">, where this label is <"+ this.getValue().get(gd.NODE_LABEL).toString() + ">");
                    boolean isLabelUpdated = setLocalVertexLabel(labelOrdering);
                    ifUpdate = ifUpdate || isLabelUpdated;
                    //System.out.println("super step ["+context.getSuperstep()+"], vertex_"+this.getId()+"'s label is updated? "+ isLabelUpdated + ". Now the label is <" + this.getValue().get(gd.NODE_LABEL).toString() + ">");
                }
            }

            if(ifUpdate){
                long index = 1;
                String vertexLabel = this.getValue().get(gd.NODE_LABEL).toString();
                if(this.hasEdges()) {
                    for (Edge<Text, MapWritable> edge : this.getEdges()) {
                        MapWritable msgBody = new MapWritable();
                        if ("null".equals(vertexLabel)) {
                            msgBody.put(this.getId(), new Text(String.valueOf(index)));
                        } else {
                            msgBody.put(this.getId(), new Text(vertexLabel + "," + index));
                        }
                        context.sendMessage(edge.getDestVertexId(), msgBody);
                        index++;
                    }
                }
            }
        }
        voteToHalt();
    }

}
