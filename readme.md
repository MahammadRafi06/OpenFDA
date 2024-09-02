# End-to-End Data Analysis Project on Food and Drug Safety (FDS) Data

## Overview

This project involves an end-to-end data analysis of the Food and Drug Safety (FDS) data available on the OpenFDA platform. The FDA provides public access to various datasets, including Adverse Drug Reactions, Device Events, COVID-19 data, and Drug/Device approvals. The primary aim of this project was to analyze this massive data to extract valuable insights.

## Project Workflow

### 1. Data Ingestion

- **Data Source**: The data is provided by FDA through APIs on the OpenFDA platform.
- **Tools Used**: Azure Data Factory (ADF)
- **Process**: 
  - I used Azure Data Factory (ADF) to ingest the data. ADF's Lookup activity was utilized to identify and manage the API endpoints.
  - Data was pulled into Azure Blob Storage using the Copy Data activity with HTTP Linked Services.
  
### 2. Data Cleaning

- **Tools Used**: Python on Databricks
- **Process**:
  - The data provided by FDA is in the form of deeply nested JSON files. I implemented a data cleaning step using Python on Databricks to transform the raw JSON files into a more manageable format.
  
### 3. Data Pre-processing and Processing

- **Tools Used**: Databricks (with Unity Catalog enabled workspace), PySpark, SparkSQL
- **Process**:
  - Created a **Bronze Layer** by populating tables from the cleaned JSON files in Blob Storage.
  - Used PySpark's `StructType`, `explode` function, and other key methods to handle and convert unstructured JSON data into structured SQL tables.
  - Created a **Silver Layer** by processing data from Bronze tables.
  - Finally, created **Gold Tables** for each data category, including:
    - Animal Adverse Events
    - Human Drug Adverse Events
    - Device Events
    - Tobacco Problems

### 4. Data Visualization

- **Tools Used**: PowerBI
- **Process**:
  - Connected Databricks to PowerBI to visualize the data and generate insightful reports.

### 5. Continuous Integration/Continuous Deployment (CI/CD)

- **Tools Used**: Azure DevOps
- **Process**:
  - Implemented build and release pipelines for CI/CD using Azure DevOps.
  - Scheduled ADF jobs with trigger mechanisms to pull data on a weekly basis, ensuring that the dataset is always up-to-date.

## Conclusion

This project showcases a robust implementation of an end-to-end data analysis pipeline, leveraging various Azure tools and platforms. From data ingestion and processing to visualization and CI/CD, this project covers the entire lifecycle of data analysis.

## Technologies Used

- **Azure Data Factory (ADF)**
- **Azure Blob Storage**
- **Databricks**
- **PySpark**
- **SparkSQL**
- **PowerBI**
- **Azure DevOps**

## How to Use

1. Clone this repository.
2. Follow the instructions in the notebooks to set up your Azure environment.
3. Deploy the provided ADF pipelines and Databricks notebooks.
4. Connect Databricks to PowerBI for visualization.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

For any queries or discussions, feel free to reach out via mrafi@sjmsom.in.
