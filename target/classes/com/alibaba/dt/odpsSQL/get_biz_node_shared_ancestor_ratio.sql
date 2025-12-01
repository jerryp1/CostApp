
DROP TABLE IF EXISTS dwd_src_vertex_name_to_id_tmp_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS dwd_src_vertex_name_to_id_tmp_${bizdate}_${task_id}
LIFECYCLE 3 AS
SELECT  src_vertex
,ROW_NUMBER() OVER (PARTITION BY 1 ) AS rn
FROM    (
  SELECT  DISTINCT src_vertex
  FROM    (
    SELECT  SPLIT_PART(apportion_vertex,"+",1) AS src_vertex
    ,SUM(ration) AS src_share_to_sink_total_ration
    FROM    (
      SELECT  vertex_name
      ,SUM(ration) / 100 AS ration
      ,apportion_vertex
      FROM    cycle_broken_graph_each_vertex_with_ancestor_apportion_ration
      WHERE   ds = '${bizdate}'
      AND     task_id = '${task_id}'
      AND     apportion_vertex LIKE '%+src'
      AND     vertex_name LIKE '%+sink'
      GROUP BY vertex_name
      ,apportion_vertex
    )
    WHERE   ration <> 1.0
    AND     ration > 0
    GROUP BY SPLIT_PART(apportion_vertex,"+",1)
  )
  WHERE   src_share_to_sink_total_ration < 1
)
;

DROP TABLE IF EXISTS dwd_biz_vertex_name_to_id_tmp_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS dwd_biz_vertex_name_to_id_tmp_${bizdate}_${task_id}
LIFECYCLE 3 AS
SELECT  biz_vertex AS biz_vertex
,ROW_NUMBER() OVER (PARTITION BY 1 ) AS rn
FROM    (
  SELECT  DISTINCT biz_vertex
  FROM    (
    SELECT  DISTINCT vertex_name AS biz_vertex
    ,SPLIT_PART(apportion_vertex,"+",1) AS src_vertex
    FROM    (
      SELECT  vertex_name
      ,SUM(ration) / 100 AS ration
      ,apportion_vertex
      FROM    cycle_broken_graph_each_vertex_with_ancestor_apportion_ration
      WHERE   ds = '${bizdate}'
      AND     task_id = '${task_id}'
      AND     apportion_vertex LIKE '%+src'
      AND     vertex_name NOT LIKE '%+sink'
      AND     vertex_name NOT LIKE '%+src'
      GROUP BY vertex_name
      ,apportion_vertex
    )
    WHERE   ration <> 1.0
    AND     ration > 0
  ) a
  JOIN    (
    SELECT  DISTINCT src_vertex
    FROM    dwd_src_vertex_name_to_id_tmp_${bizdate}_${task_id}
  ) b
  ON      a.src_vertex = b.src_vertex
)
;


SET odps.isolation.session.enable = TRUE
;

SET odps.sql.udf.jvm.memory = 12288
;

SET odps.sql.mapper.cpu = 800
;

SET odps.sql.mapper.memory = 12288
;

set odps.sql.reducer.memory=12288;

set odps.sql.reducer.cpu=800;

DROP TABLE IF EXISTS dwd_apportion_matrix_item2_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS dwd_apportion_matrix_item2_${bizdate}_${task_id}
LIFECYCLE 7 AS
SELECT  SPLIT_PART(record,"::",1) AS src_id
,SPLIT_PART(record,"::",2) AS sink_id
,SPLIT_PART(record,"::",3) AS ratio
FROM    (
--the implement of the udf refer to the class GetItem2
  SELECT  get_matrix_item2(matric_a,row_num,column_num) AS ret_matrix
  FROM    (
  -- matrix A
    SELECT  COLLECT_LIST(CONCAT(dst_id,"::",src_id,"::",ration)) AS matric_a
    ,COUNT(DISTINCT dst_id) AS row_num
    ,COUNT(DISTINCT src_id) AS column_num
    FROM    (
      SELECT  b.rn AS dst_id
      ,c.rn AS src_id
      ,ration
      FROM    (
        SELECT  DISTINCT SPLIT_PART(apportion_vertex,"+",1) AS src_guid
        ,SPLIT_PART(vertex_name,"+",1) AS dst_guid
        ,ration
        FROM    (
          SELECT  vertex_name
          ,SUM(ration) / 100 AS ration
          ,apportion_vertex
          FROM    cycle_broken_graph_each_vertex_with_ancestor_apportion_ration
          WHERE   ds = '${bizdate}'
          AND     task_id = '${task_id}'
          AND     apportion_vertex LIKE '%+src'
          AND     vertex_name LIKE '%+sink'
          GROUP BY vertex_name
          ,apportion_vertex
        )
        WHERE   ration > 0
      ) a
      JOIN    (
        SELECT  *
        FROM    dwd_src_vertex_name_to_id_tmp_${bizdate}_${task_id}
      ) b
      ON      a.dst_guid = b.src_vertex
      JOIN    (
        SELECT  *
        FROM    dwd_src_vertex_name_to_id_tmp_${bizdate}_${task_id}
      ) c
      ON      a.src_guid = c.src_vertex
    )
  )
)
LATERAL VIEW EXPLODE(ret_matrix) a AS record
;

DROP TABLE IF EXISTS dwd_biz_node_share_src_apportion_ratio_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS dwd_biz_node_share_src_apportion_ratio_${bizdate}_${task_id}
LIFECYCLE 7 AS
SELECT  biz_vertex
,src_vertex
,ratio
FROM    (
  SELECT  a.biz_id
  ,b.sink_id
  ,SUM(a.ration * b.ratio) AS ratio
  FROM    (
    SELECT  b.rn AS biz_id
    ,c.rn AS src_id
    ,ration * 0.5 AS ration
    FROM    (
      SELECT  DISTINCT SPLIT_PART(apportion_vertex,"+",1) AS src_guid
      ,vertex_name AS dst_guid
      ,ration
      FROM    (
        SELECT  vertex_name
        ,SUM(ration) / 100 AS ration
        ,apportion_vertex
        FROM    cycle_broken_graph_each_vertex_with_ancestor_apportion_ration
        WHERE   ds = '${bizdate}'
        AND     task_id = '${task_id}'
        AND     apportion_vertex LIKE '%+src'
        AND     vertex_name NOT LIKE '%+sink'
        AND     vertex_name NOT LIKE '%+src'
        AND     vertex_name NOT LIKE '%+src'
        GROUP BY vertex_name
        ,apportion_vertex
      )
      WHERE   ration > 0
    ) a
    JOIN    (
      SELECT  *
      FROM    dwd_biz_vertex_name_to_id_tmp_${bizdate}_${task_id}
    ) b
    ON      a.dst_guid = b.biz_vertex
    JOIN    (
      SELECT  *
      FROM    dwd_src_vertex_name_to_id_tmp_${bizdate}_${task_id}
    ) c
    ON      a.src_guid = c.src_vertex
  ) a
  JOIN    (
    SELECT  src_id
    ,sink_id
    ,ratio
    FROM    dwd_apportion_matrix_item2_${bizdate}_${task_id} a
    JOIN    (
      SELECT  *
      FROM    dwd_src_vertex_name_to_id_tmp_${bizdate}_${task_id}
    ) b
    ON      a.src_id = b.rn
    JOIN    (
      SELECT  *
      FROM    dwd_src_vertex_name_to_id_tmp_${bizdate}_${task_id}
    ) c
    ON      a.sink_id = c.rn
  ) b
  ON      a.src_id = b.src_id
  GROUP BY a.biz_id
  ,b.sink_id
) a
LEFT OUTER JOIN (
  SELECT  *
  FROM    dwd_biz_vertex_name_to_id_tmp_${bizdate}_${task_id}
) b
ON      a.biz_id = b.rn
LEFT OUTER JOIN (
  SELECT  *
  FROM    dwd_src_vertex_name_to_id_tmp_${bizdate}_${task_id}
) c
ON      a.sink_id = c.rn
;

DROP TABLE IF EXISTS dwd_biz_node_share_accestor_ratio_${bizdate}_${task_id}
;

CREATE TABLE IF NOT EXISTS dwd_biz_node_share_accestor_ratio_${bizdate}_${task_id}
LIFECYCLE 7 AS
SELECT  biz_vertex
,apportion_vertex
,SUM(ratio) AS ratio
FROM    (
  SELECT  biz_vertex
  ,b.apportion_vertex
  ,ratio * cost AS ratio
  FROM    (
    SELECT  biz_vertex
    ,src_vertex
    ,ratio
    FROM    dwd_biz_node_share_src_apportion_ratio_${bizdate}_${task_id}
  ) a
  LEFT OUTER JOIN (
    SELECT  SPLIT_PART(vertex_name,"+",1) AS src_vertex
    ,SUM(ration) / 100 AS cost
    ,apportion_vertex
    FROM    cycle_broken_graph_each_vertex_with_ancestor_apportion_ration
    WHERE   ds = '${bizdate}'
    AND     task_id = '${task_id}'
    AND     vertex_name LIKE '%+sink'
    AND     apportion_vertex NOT LIKE '%+src'
    AND     apportion_vertex NOT LIKE '%+sink'
    AND     ration > 0
    GROUP BY SPLIT_PART(vertex_name,"+",1)
    ,apportion_vertex
  ) b
  ON      a.src_vertex = b.src_vertex
  UNION ALL
  SELECT  vertex_name AS biz_vertex
  ,apportion_vertex
  ,SUM(ration) / 100 AS cost
  FROM    cycle_broken_graph_each_vertex_with_ancestor_apportion_ration
  WHERE   ds = '${bizdate}'
  AND     task_id = '${task_id}'
  AND     vertex_name NOT LIKE '%+sink'
  AND     vertex_name NOT LIKE '%+src'
  AND     apportion_vertex NOT LIKE '%+src'
  AND     apportion_vertex NOT LIKE '%+sink'
  AND     ration > 0
  GROUP BY vertex_name
  ,apportion_vertex
)
GROUP BY biz_vertex
,apportion_vertex
;

CREATE TABLE IF NOT EXISTS dwd_biz_node_share_accestor_ratio_1d
(
  biz_vertex       STRING COMMENT 'FIELD'
,accestor_vertex STRING COMMENT 'FIELD'
,ratio           STRING COMMENT 'FIELD'
)
COMMENT 'TABLE COMMENT'
PARTITIONED BY
(
  ds               STRING COMMENT ''
,task_id         STRING COMMENT ''
)
LIFECYCLE 760
;

INSERT OVERWRITE TABLE dwd_biz_node_share_accestor_ratio_1d PARTITION (ds = '${bizdate}',task_id = '${task_id}')
SELECT  *
FROM    dwd_biz_node_share_accestor_ratio_${bizdate}_${task_id}
;