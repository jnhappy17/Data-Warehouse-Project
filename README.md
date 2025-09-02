# Data-Warehouse-Project
**Building a data warehouse using Microsoft SQL Server**

Welcome!  
This project highlights industry best practices for Data Engineering through the creation of a data warehousing solution using Microsoft SQL Server.

## Project overview
This project involves:

**Data Architecture**: Designing a Modern Data Warehouse Using the Medallion Architecture with Bronze, Silver and Gold layers.  
**ETL Pipelines**: Extracting, Transforming, and Loading data from source systems into the warehouse.  
**Data Modeling**: Developing fact and dimension tables optimized for analytical queries.  

## Specifications
**Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.  
**Data Quality**: Cleanse and resolve data quality issues prior to analysis.  
**Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.  
**Scope**: Focus on the latest dataset only; historization of data is not required.  
**Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

## Repository Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming_conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project
```

## Data Architecture
The data architecture of this project is comprised of 3 layers as follow:
<img width="801" height="441" alt="DWH project" src="https://github.com/user-attachments/assets/546c816a-42a1-40aa-b2eb-3e1b6b5eba98" />
