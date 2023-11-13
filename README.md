# Databricks Pipeline for College Basketball Statistics

## Repository Structure
```text
mjh140-databricks-pipeline/
├── NCAAB_Data_Scrape.py
├── NCAAB_SQL_Insert.py
└── README.md
```
*Note: This project highlights the automated data processing jobs configured in Azure Databricks; therefore, this repoository only contains the notebooks used in the data pipeline*

## Summary
This repository is an example of web scraping (data source) and data ingestion into a SQL database (data sink) using an end-to-end automated data pipeline in Azure databricks. The data for this project is sourced from the college basketball statistics website Kenpom.com which hosts advanced analytics on every college basketball team, game, and player in NCAA Division 1 basketball. The table scraped for this project can be found [here](https://kenpom.com/index.php) and contains data such as adjusted efficiency margin, strength of schedule rating, adjusted tempo ratings and more team-oriented stats.

## Data Pipeline

#### Step 1:
The data pipeline has two databricks notebooks: NCAAB_Data_Scrape and NCAAB_SQL_Insert. The first notebook imports the `kenpompy` python package created specifically for scraping the Kenpom website. Login credentials are also passed into `login` function and we're returned with an authenticated browser object.

![image](https://github.com/nogibjj/mjh140-databricks-pipeline/assets/114833075/43ca8005-ce44-4dec-b137-72897233cb19)

#### Step 2:
The `ratings` function from the `kenpompy` package is used to scrape the desired table into a pandas dataframe. The dataframe requires minor cleaning including datatype modifications and deletion of any html elements that were caught in the scrape. The result of the data cleaning is a tidy dataset with 362 Division 1 teams and their respective statistics.

![image](https://github.com/nogibjj/mjh140-databricks-pipeline/assets/114833075/644faa41-ecf1-4653-995b-53a11285907c)

#### Step 3: 
The pandas dataframe is now converted to a Spark DataFrame where it can be prepped for insertion into a SQL database. The picture below shows the code for setting up the dataframe schema. The final line of code establishes a global view of the dataframe so that dataframe can be accessed from different spark sessions using the same cluster. This will be necessary for passing data between notebooks in an automated data pipeline.

![image](https://github.com/nogibjj/mjh140-databricks-pipeline/assets/114833075/a256e2a2-f155-4274-88f0-6d4bada577f8)

#### Step 4:
The second notebook is used exclusively for data ingestion into a sql database. The schema from the spark dataframe is replicated for the sql insert statement. We reference data from the global_temp view of the spark dataframe. The picture below shows successful execution of the sql command with 362 rows inserted into the database.

![image](https://github.com/nogibjj/mjh140-databricks-pipeline/assets/114833075/717ca8fb-0cd8-45ed-877a-d2b03c192d31)

#### Step 5:
The final step is to automate the web scraping and data ingestion of college basketball statistics. Every day games are played - but primarily in the afternoon and evening. Therefore, we will set a reoccuring Databricks job to execute both notebooks sequentially at 8AM every day. The picture below shows the workflow job NCAAB_DailyScrape, and the respective tasks in the databricks job. The scheduled 8AM execution is highlighted on the right side of the picture.

![image](https://github.com/nogibjj/mjh140-databricks-pipeline/assets/114833075/2e1643b3-e072-41be-b05a-39d164f1338e)




