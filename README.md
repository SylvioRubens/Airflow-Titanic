Overview
========

The project was created as an activity of the discipline of Preparation, Orchestration and Data Flows of the postgraduate course in Data Engineering at PUC Minas

The project consists of creating data pipeline flows using the airflow framework, in a didactic way, creating 2 airflow Dags. In "DAG1", we consume data from the titanic dataset, running through 3 tasks in parallel, where each one calculating 1 specific indicator:
- Number of passengers by gender and class
- Average fare price paid by gender and class
- Total amount of SibSp + Parch (all together) by gender and class

At the end of DAG1, a data table is created with the tables of the 3 consolidated indicators, and then, triggering  the DAG2.

DAG2 is responsible for reading this data, and calculating the average value of each indicator from the result table of DAG1.

Project Contents
================

- dags: This folder contains the Python files for your Airflow DAGs. 
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.
- prints: Show PNG files as result of the activity, where it shows each dag flow, and the resulting table for each dag on log screen of airflow.