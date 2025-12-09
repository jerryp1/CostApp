# CostApp

This repository contains the source code, datasets for our research on product costing in data pipelines. Our full version paper is presented in [`CostApp.pdf`](https://github.com/jerryp1/CostApp/blob/main/CostApp.pdf).

## Contents

- **Source Code**:
  - `src/main/java/com/alibaba/dt/graph`: Java implementation for MaxCompute Graph processing.
  - `src/main/java/com/alibaba/dt/odpsSQL`: SQL scripts for MaxCompute workflow.
  - `src/main/java/com/alibaba/dt/udf`: User Defined Functions (UDFs).
  - `src/main/java/com/alibaba/dt/start.sh`: Shell script to execute the workflow.

- **Datasets**:
  - `resources/graph_data`: Open-source graph datasets used in the experiments.
    - *Note*: Large files are split (e.g., `*_xa`, `*_xb`). Please concatenate them (e.g., `cat file_xa file_xb > file.zip`) and unzip before use.

## Usage

The project is designed to run on Alibaba Cloud MaxCompute.

1.  **Build**: Use Maven to package the Java code (Graph and UDFs).
2.  **Run**:
    - **Option 1**: Create MaxCompute SQL nodes and schedule them in the order: `to_break_cycle` -> `get_node_apportion_ration` -> `get_biz_node_shared_ancestor_ratio` -> `cost_apportion_final`.
    - **Option 2**: Upload SQL files and use the provided `start.sh` script.

## Requirements

- Java 8
- Maven
- Alibaba Cloud MaxCompute environment
