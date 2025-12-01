
CREATE TABLE IF NOT EXISTS dwd_cost_apportion_src_to_leaf_ratio_1d
(
  leaf_biz_node    STRING COMMENT ''
,ancestor_node   STRING COMMENT ''
,apportion_ratio STRING COMMENT ''
)
COMMENT 'TABLE COMMENT'
PARTITIONED BY
(
  ds               STRING COMMENT '分区'
,task_id         STRING COMMENT '任务'
)
LIFECYCLE 760
;

INSERT OVERWRITE TABLE dwd_cost_apportion_src_to_leaf_ratio_1d PARTITION (ds = '${bizdate}',task_id = '${task_id}')
SELECT  DISTINCT guid AS leaf
,guid AS src
,'1' AS ratio
FROM    cycle_broken_graph_vertex_set
WHERE   ds = '${bizdate}'
AND     task_id = '${task_id}'
AND     guid NOT LIKE '%+sink'
AND     guid NOT LIKE '%+src'
AND     guid NOT IN (
  SELECT  DISTINCT biz_vertex
  FROM    dwd_biz_node_share_accestor_ratio_1d
  WHERE   ds = '${bizdate}'
  AND     task_id = '${task_id}'
  UNION
  SELECT  DISTINCT accestor_vertex
  FROM    dwd_biz_node_share_accestor_ratio_1d
  WHERE   ds = '${bizdate}'
  AND     task_id = '${task_id}'
)
UNION
SELECT  biz_vertex
,accestor_vertex
,ratio
FROM    dwd_biz_node_share_accestor_ratio_1d
WHERE   ds = '${bizdate}'
AND     task_id = '${task_id}'
;