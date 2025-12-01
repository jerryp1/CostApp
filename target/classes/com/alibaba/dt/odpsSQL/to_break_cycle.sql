
DROP TABLE IF EXISTS to_break_cycle_graph_edge_set_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS to_break_cycle_graph_edge_set_${bizdate}_${task_id}
LIFECYCLE 7 AS
SELECT  "START_VIRTUAL_NODE" AS src_guid
        ,a.src_guid AS dst_guid
FROM    (
            SELECT  DISTINCT src_guid
            FROM    ${edge_table}
        ) a
LEFT ANTI JOIN  (
                    SELECT  DISTINCT dst_guid
                    FROM    ${edge_table}
                ) b
ON      a.src_guid = b.dst_guid
UNION
SELECT  DISTINCT src_guid
        ,dst_guid
FROM    ${edge_table}
;

DROP TABLE IF EXISTS to_break_cycle_graph_vertex_set_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS to_break_cycle_graph_vertex_set_${bizdate}_${task_id}
LIFECYCLE 7 AS
SELECT  DISTINCT src_guid AS guid
FROM    to_break_cycle_graph_edge_set_${bizdate}_${task_id}
UNION
SELECT  DISTINCT dst_guid
FROM    to_break_cycle_graph_edge_set_${bizdate}_${task_id}
;

--********************************************************************--
--lexicographic ordering
--********************************************************************--
CREATE TABLE IF NOT EXISTS to_break_cycle_graph_dfs_ordering_ret_${bizdate}_${task_id}
(
    guid        STRING COMMENT 'FIELD'
    ,lexi_LABEL STRING COMMENT 'FIELD'
)
COMMENT '${biz_name}_node_cost_graph_vertex_dfs_ordering'
LIFECYCLE 7
;

SET graphscope.mode = OdpsGraphOnly
;

SET odps.graph.job.priority = 0
;

SET odps.graph.fuxi.job.max.wait.time = 1000
;

SET odps.graph.worker.num = 800
;

SET odps.graph.worker.memory = 25768
;

SET odps.graph.use.multiple.input.output = TRUE
;

SET odps.graph.computing.threads = 100
;

SET odps.graph.worker.cpu = 300
;

SET odps.graph.max.attempts = 1000
;
--one has to package the odps graph class and submit into the server.
--@resource_reference{"DFSOrderingViaLBFS.jar"}
JAR -RESOURCES DFSOrderingViaLBFS.JAR -classpath ./DFSOrderingViaLBFS.jar
com.alibaba.dt.graph.DFSOrderingViaLBFS.DFSOrderingViaLBFS
to_break_cycle_graph_vertex_set_${bizdate}_${task_id}
to_break_cycle_graph_edge_set_${bizdate}_${task_id}
to_break_cycle_graph_dfs_ordering_ret_${bizdate}_${task_id} 1000;

--********************************************************************--
--get fas
--********************************************************************--
DROP TABLE IF EXISTS to_break_cycle_graph_fas_set_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS to_break_cycle_graph_fas_set_${bizdate}_${task_id}
LIFECYCLE 7 AS
SELECT  *
FROM    (
SELECT  a.src_guid
,b.lexi_label AS src_lexi_label
,a.dst_guid
,c.lexi_label AS dst_lexi_label
-- implement of the udf is the class LexiLabelCompare
,COMPARE_LEXICOGRAPHY_LABEL(c.lexi_label,b.lexi_label,'strict') AS strict_type
FROM    (
SELECT  DISTINCT src_guid
,dst_guid
FROM    ${edge_table}
) a
JOIN    (
SELECT  *
FROM    to_break_cycle_graph_dfs_ordering_ret_${bizdate}_${task_id}
WHERE   lexi_label <> 'null'
) b
ON      a.src_guid = b.guid
JOIN    (
SELECT  *
FROM    to_break_cycle_graph_dfs_ordering_ret_${bizdate}_${task_id}
WHERE   lexi_label <> 'null'
) c
ON      a.dst_guid = c.guid
)
WHERE   strict_type = '1'
;

--********************************************************************--
--cycle free graph
--********************************************************************--
CREATE TABLE IF NOT EXISTS cycle_broken_graph_edge_set
(
src_guid  STRING COMMENT 'FIELD'
,dst_guid STRING COMMENT 'FIELD'
,ratio    STRING COMMENT 'FIELD'
)
COMMENT 'TABLE COMMENT'
PARTITIONED BY
(
ds        STRING COMMENT '分区'
,task_id  STRING COMMENT ''
)
LIFECYCLE 360
;

CREATE TABLE IF NOT EXISTS cycle_broken_graph_vertex_set
(
guid     STRING COMMENT 'FIELD'
)
COMMENT 'TABLE COMMENT'
PARTITIONED BY
(
ds       STRING COMMENT '分区'
,task_id STRING COMMENT ''
)
LIFECYCLE 360
;

INSERT OVERWRITE TABLE cycle_broken_graph_edge_set PARTITION (ds = '${bizdate}',task_id = '${task_id}')
SELECT  DISTINCT src_guid
,CONCAT(src_guid,"-",dst_guid,"+sink") AS dst_guid
,'1' AS ratio
FROM    to_break_cycle_graph_fas_set_${bizdate}_${task_id}
UNION
SELECT  DISTINCT CONCAT(src_guid,"-",dst_guid,"+src") AS src_guid
,dst_guid
,'1' AS ratio
FROM    to_break_cycle_graph_fas_set_${bizdate}_${task_id}
UNION
SELECT  DISTINCT a.*
FROM    (
SELECT  a.*
FROM    (
SELECT  DISTINCT src_guid
,dst_guid
,ratio
FROM    ${edge_table}
) a
JOIN    (
SELECT  *
FROM    to_break_cycle_graph_dfs_ordering_ret_${bizdate}_${task_id}
WHERE   lexi_label <> 'null'
) b
ON      a.src_guid = b.guid
JOIN    (
SELECT  *
FROM    to_break_cycle_graph_dfs_ordering_ret_${bizdate}_${task_id}
WHERE   lexi_label <> 'null'
) c
ON      a.dst_guid = c.guid
) a
LEFT ANTI JOIN  (
SELECT  *
FROM    to_break_cycle_graph_fas_set_${bizdate}_${task_id}
) b
ON      a.src_guid = b.src_guid
AND     a.dst_guid = b.dst_guid
;

INSERT OVERWRITE TABLE cycle_broken_graph_vertex_set PARTITION (ds = '${bizdate}',task_id = '${task_id}')
SELECT  DISTINCT src_guid AS guid
FROM    cycle_broken_graph_edge_set
WHERE   ds = '${bizdate}'
AND     task_id = '${task_id}'
UNION
SELECT  DISTINCT dst_guid
FROM    cycle_broken_graph_edge_set
WHERE   ds = '${bizdate}'
AND     task_id = '${task_id}'
;