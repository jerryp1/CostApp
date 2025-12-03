# cost_apportion

## The code runs on the MaxCompute(known as ODPS).
You could repeat the experiment in two ways:

1) create four ODPS-SQL-Nodes in some project of MaxCompute, where the SQLs are given in /src/main/java/com/alibaba/dt/odpsSQL.
   The scheduling dependencies of the four nodes are <to_break_cycle> -> <get_node_apportion_ration> -> <get_biz_node_shared_ancestor_ratio> -> <cost_apportion_final>, which are given from source to sink.
   An input are required by <to_break_cycle>, which is a table comprised of 3 columns, named as "src_guid", "dst_guid", "ratio".
   
2) upload the four SQL files as resources in some project of MaxCompute, and create a Shell-Node (/src/main/java/com/alibaba/dt/start.sh).
   The input of the Shell-Node is a table mentioned above.

Moreover, you have to package four Jar-packages, two of them are for ODPS_GRAPH process and the other two are used as UDF, the codes are given in /src/main/java/com/alibaba/dt/graph and  /src/main/java/com/alibaba/dt/udf respectively.

## The Data

We also provide 12 graphs, which can be classified into 4 groups: S_N, S_T, M_N and B_N.
The series of numbers at the end of the name are related to the number of vertices given in Table 1 of the submission.

As you can see, there are two kinds of files in the data profile.
1) *.zip : You may unzip it and get the whole graph.
2) *_xa, *_xb : Please cat files with the same prefix into a zip file, i.e.,
         cat <graph_name>_x* > <final_zip_file_name>.zip ,
   and then unzip it.
for example, if you want to get the graph <GRAPH_B_N_281819253>

run  cat GRAPH_B_N_28181925_x* > file.zip ,
unzip file.zip.

