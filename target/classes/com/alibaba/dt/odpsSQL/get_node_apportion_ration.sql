
DROP TABLE IF EXISTS cycle_broken_graph_vertex_set_to_rn_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS cycle_broken_graph_vertex_set_to_rn_${bizdate}_${task_id}
LIFECYCLE 7 AS
SELECT  guid
        ,to_char(ROW_NUMBER() OVER (PARTITION BY 1 )) AS rn
FROM    (
            SELECT  DISTINCT guid
            FROM    cycle_broken_graph_vertex_set
            WHERE   ds = '${bizdate}'
            AND     task_id = '${task_id}'
        )
;

DROP TABLE IF EXISTS cycle_broken_graph_vertex_set_extend_cost_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS cycle_broken_graph_vertex_set_extend_cost_${bizdate}_${task_id}
LIFECYCLE 7 AS
SELECT  b.rn AS guid
        ,'100' AS cost
        ,'false' AS if_leaf
FROM    (
            SELECT  DISTINCT guid
            FROM    cycle_broken_graph_vertex_set
            WHERE   ds = '${bizdate}'
            AND     task_id = '${task_id}'
        ) a
JOIN    cycle_broken_graph_vertex_set_to_rn_${bizdate}_${task_id} b
ON      a.guid = b.guid
;

DROP TABLE IF EXISTS cycle_broken_graph_edge_set_extend_ration_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS cycle_broken_graph_edge_set_extend_ration_${bizdate}_${task_id}
LIFECYCLE 7 AS
SELECT  b.rn AS src_guid
        ,c.rn AS dst_guid
        ,a.ratio
FROM    (
            SELECT  src_guid
                    ,dst_guid
                    ,ratio
            FROM    cycle_broken_graph_edge_set
            WHERE   ds = '${bizdate}'
            AND     task_id = '${task_id}'
        ) a
JOIN    cycle_broken_graph_vertex_set_to_rn_${bizdate}_${task_id} b
ON      a.src_guid = b.guid
JOIN    cycle_broken_graph_vertex_set_to_rn_${bizdate}_${task_id} c
ON      a.dst_guid = c.guid
;

DROP TABLE IF EXISTS cycle_broken_graph_each_vertex_with_ancestor_apportion_ration_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS cycle_broken_graph_each_vertex_with_ancestor_apportion_ration_${bizdate}_${task_id}
(
    vertex_name       STRING COMMENT 'FIELD'
    ,apportion_vertex STRING COMMENT 'FIELD'
    ,ration           STRING COMMENT 'FIELD'
)
COMMENT ''
LIFECYCLE 3
;

SET graphscope.mode = OdpsGraphOnly
;

SET odps.graph.job.priority = 0
;

SET odps.graph.fuxi.job.max.wait.time = 1000
;

SET odps.graph.worker.num = 800
;

SET odps.graph.worker.memory = 30768
;

SET odps.graph.use.multiple.input.output = TRUE
;

SET odps.graph.computing.threads = 100
;

SET odps.graph.worker.cpu = 800
;

--@resource_reference{"Distributed-Graph-Compute.jar"}
--one has to package the odps graph class and submit into the server.
JAR -RESOURCES Distributed-Graph-Compute.jar -classpath ./Distributed-Graph-Compute.jar
com.alibaba.dt.graph.DAGCostApportionDetailByLevel.Main
cycle_broken_graph_vertex_set_extend_cost_${bizdate}_${task_id}
cycle_broken_graph_edge_set_extend_ration_${bizdate}_${task_id}
cycle_broken_graph_each_vertex_with_ancestor_apportion_ration_${bizdate}_${task_id} 10000;

CREATE TABLE IF NOT EXISTS cycle_broken_graph_each_vertex_with_ancestor_apportion_ration
(
vertex_name       STRING COMMENT 'FIELD'
,apportion_vertex STRING COMMENT 'FIELD'
,ration           DOUBLE COMMENT 'FIELD'
)
COMMENT 'TABLE COMMENT'
PARTITIONED BY
(
ds                STRING COMMENT '分区'
,task_id          STRING COMMENT ''
)
LIFECYCLE 60
;

INSERT OVERWRITE TABLE cycle_broken_graph_each_vertex_with_ancestor_apportion_ration PARTITION (ds = '${bizdate}',task_id = '${task_id}')
SELECT  b.guid AS vertex_name
,c.guid AS apportion_vertex
,a.ration
FROM    (
SELECT  vertex_name
,apportion_vertex
,SUM(ration) AS ration
FROM    cycle_broken_graph_each_vertex_with_ancestor_apportion_ration_${bizdate}_${task_id}
GROUP BY vertex_name
,apportion_vertex
) a
JOIN    cycle_broken_graph_vertex_set_to_rn_${bizdate}_${task_id} b
ON      a.vertex_name = b.rn
JOIN    cycle_broken_graph_vertex_set_to_rn_${bizdate}_${task_id} c
ON      a.apportion_vertex = c.rn
;
