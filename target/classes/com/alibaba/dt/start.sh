#!/bin/bash
#param define
#********************************************************************#
# this is an odps wrapper
odpswrapper="/opt/taobao/tbdpapp/odpswrapper/odpswrapper.py"
#input the address of the sql file on the server
to_break_cycle=
get_node_apportion_ration=
get_biz_node_shared_ancestor_ratio=
cost_apportion_final=
bizdate=$1
#input a non-partition table contains column src_guid, dst_guid and ratio，src_guid+dst_guid should be unique
edge_table=""
# a task name defined by user
job_id=""
#********************************************************************#
# the algorithm runs in 4 steps
# 1 break cycle
# 2 apportion the ratio on the cycle free graph
# 3 get the matrix and find out the finnal tranform ratios between source vertices and sink vertices
# 4 get the final apportion result
#********************************************************************#
${odpswrapper} -f ${to_break_cycle} "bizdate=$bizdate" "edge_table=$edge_table" "task_id=$job_id"
statusa=$?
if [ "x$statusa" != "x0" ];then
    echo "job [to_break_cycle] of $$job_id is failed once!"
    ${odpswrapper} -f ${to_break_cycle} "bizdate=$bizdate" "edge_table=$edge_table" "task_id=$job_id"
    statusa1=$?
    if [ "x$statusa1" != "x0" ];then
        echo "job [to_break_cycle] of $$job_id is failed twice!"
        exit 1
    fi
fi

${odpswrapper} -f ${get_node_apportion_ration} "bizdate=$bizdate" "task_id=$job_id"
statusb=$?
if [ "x$statusb" != "x0" ];then
    echo "job [get_node_apportion_ration] of $$job_id is failed once!"
    ${odpswrapper} -f ${get_node_apportion_ration} "bizdate=$bizdate" "task_id=$job_id"
    statusb1=$?
    if [ "x$statusb1" != "x0" ];then
        echo "job [get_node_apportion_ration] of $$job_id is failed twice!"
        exit 1
    fi
fi

${odpswrapper} -f ${get_biz_node_shared_ancestor_ratio} "bizdate=$bizdate" "task_id=$job_id"
statusc=$?
if [ "x$statusc" != "x0" ];then
    echo "job [get_biz_node_shared_ancestor_ratio] of $$job_id is failed once!"
    ${odpswrapper} -f ${get_biz_node_shared_ancestor_ratio} "bizdate=$bizdate" "task_id=$job_id"
    statusc1=$?
    if [ "x$statusc1" != "x0" ];then
        echo "job [get_biz_node_shared_ancestor_ratio] of $$job_id is failed twice!"
        exit 1
    fi
fi


${odpswrapper} -f ${cost_apportion_final} "bizdate=$bizdate" "task_id=$job_id"
statusd=$?
if [ "x$statusd" != "x0" ];then
    echo "job [cost_apportion_final] of $$job_id is failed once!"
    ${odpswrapper} -f ${cost_apportion_final} "bizdate=$bizdate" "task_id=$job_id"
    statusd1=$?
    if [ "x$statusd1" != "x0" ];then
        echo "job [cost_apportion_final] of $$job_id is failed twice!"
        exit 1
    fi
fi

exit 0
