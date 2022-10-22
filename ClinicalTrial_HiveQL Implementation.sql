-- Databricks notebook source
-- MAGIC %md
-- MAGIC HiveQL Implementation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Setting the variables to allow for reusablity

-- COMMAND ----------

set hivevar:x= clinicaltrial_2021;

-- COMMAND ----------

set hivevar:y= mesh;

-- COMMAND ----------

set hivevar:z= pharma;

-- COMMAND ----------

set hivevar:year= %2021;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dropping previously produced views to ensure that a new view with the right data is created, allowing for reusability.

-- COMMAND ----------

DROP VIEW IF EXISTS NewConditions;
DROP VIEW IF EXISTS MonthlyView;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating the three tables and viewing their contents

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:x}(
Id STRING ,
Sponsor STRING ,
Status STRING ,
Start STRING ,
Completion STRING ,
Type STRING ,
Submission STRING ,
Conditions STRING ,
Interventions DATE )
USING CSV
OPTIONS (path '/FileStore/tables/${hivevar:x}', delimiter "|", header 'true' )

-- COMMAND ----------

SELECT * FROM ${hivevar:x}

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:y}(
Term STRING,
Tree STRING)
USING CSV
OPTIONS (path '/FileStore/tables/${hivevar:y}', delimiter ",", header 'true' )

-- COMMAND ----------

SELECT * FROM ${hivevar:y}

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:z}(
Company STRING,
Parent_Company STRING,
Penalty_Amount STRING,
Subtraction_From_Penalty STRING,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING,
Penalty_Year STRING,
Penalty_Date STRING,
Offense_Group STRING,
Primary_Offense STRING,
Secondary_Offense STRING,
Description STRING,
Level_of_Government STRING,
Action_Type STRING,
Agency STRING,
Civil_Criminal STRING,
Prosecution_Agreement STRING,
Court STRING,
Case_ID STRING,
Private_Litigation_Case_Title STRING,
Lawsuit_Resolution STRING,
Facility_State STRING,
City STRING,
Address STRING,
Zip STRING,
NAICS_Code STRING,
NAICS_Translation STRING,
HQ_Country_of_Parent STRING,
HQ_State_of_Parent STRING,
Ownership_Structure STRING,
Parent_Company_Stock_Ticker STRING,
Major_Industry_of_Parent STRING,
Specific_Industry_of_Parent STRING,
Info_Source STRING,
Notes STRING)
USING CSV
OPTIONS (path '/FileStore/tables/${hivevar:z}', delimiter ",", header 'true' )

-- COMMAND ----------

SELECT * FROM ${hivevar:z}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 1: Counting number of studies in the dataset using the count() function

-- COMMAND ----------

SELECT DISTINCT COUNT(*) AS Studies
FROM ${hivevar:x}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 2: list all the types of studies in the dataset along with the frequencies of each type ordered from most frequent to least frequent

-- COMMAND ----------

SELECT Type, COUNT(Type) As Total
FROM ${hivevar:x}
GROUP BY Type
ORDER BY Total DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 3: Getting the top 5 conditions (from Conditions) with their frequencies.

-- COMMAND ----------

SELECT Most_Conditions, count(*) AS Total
FROM ${hivevar:x}
LATERAL VIEW explode(split(Conditions, ",")) AS Most_Conditions
GROUP BY Most_Conditions
ORDER BY Total DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 4: The 5 most frequent roots

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating a view NewConditions using lateral view explode

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS NewConditions AS SELECT Most_Conditions
FROM ${hivevar:x}
LATERAL VIEW explode(split(Conditions, ",")) AS Most_Conditions

-- COMMAND ----------

SELECT LEFT (${hivevar:y}.tree,3) AS Roots, count(*) AS Frequency
FROM ${hivevar:y}
JOIN NewConditions
ON NewConditions.Most_Conditions=${hivevar:y}.term
GROUP BY Roots
ORDER BY Frequency DESC
LIMIT 5

-- COMMAND ----------

SELECT LEFT (${hivevar:y}.tree,3) AS Roots, COUNT(*) AS Frequency
FROM ${hivevar:y}
JOIN NewConditions
ON NewConditions.Most_Conditions=${hivevar:y}.term
GROUP BY Roots
ORDER BY Frequency DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 5: Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored. 

-- COMMAND ----------

SELECT ${hivevar:x}.sponsor, COUNT(*) AS frequency
FROM ${hivevar:x}
LEFT ANTI JOIN ${hivevar:z}
ON ${hivevar:x}.sponsor=${hivevar:z}.Parent_company
GROUP BY ${hivevar:x}.sponsor
ORDER BY frequency DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 6: Plot number of completed studies each month in a given year

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS MonthlyView AS
SELECT unix_timestamp(left (Completion, 3), "MMM") AS Months, COUNT(*) AS Total
FROM ${hivevar:x}
WHERE Status= "Completed" and Completion Like "${hivevar:year}"
GROUP BY Completion
ORDER BY Months;

-- COMMAND ----------

SELECT from_unixtime(Months, "MMM") as Months,Total
FROM MonthlyView

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A review of the headquarter country of the parent company to confirm the top countries sponsoring the project.

-- COMMAND ----------

SELECT ${hivevar:x}.sponsor, ${hivevar:z}.HQ_Country_of_Parent, COUNT(${hivevar:x}.sponsor) AS frequency
FROM ${hivevar:x}
FULL JOIN ${hivevar:z}
ON ${hivevar:x}.sponsor=${hivevar:z}.Parent_company
GROUP BY ${hivevar:x}.sponsor, ${hivevar:z}.HQ_Country_of_Parent
ORDER BY frequency DESC
LIMIT 10;

-- COMMAND ----------

SELECT ${hivevar:x}.sponsor, ${hivevar:z}.HQ_Country_of_Parent, COUNT(${hivevar:x}.sponsor) AS frequency
FROM ${hivevar:x}
FULL JOIN ${hivevar:z}
ON ${hivevar:x}.sponsor=${hivevar:z}.Parent_company
GROUP BY ${hivevar:x}.sponsor, ${hivevar:z}.HQ_Country_of_Parent
ORDER BY frequency DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A review of the primary offense in the pharmaceutical violations and the sponsors responsible.

-- COMMAND ----------

SELECT DISTINCT ${hivevar:z}.Primary_Offense, ${hivevar:x}.sponsor, COUNT (${hivevar:x}.sponsor) AS Frequency
FROM ${hivevar:x}
FULL JOIN ${hivevar:z}
ON ${hivevar:x}.sponsor=${hivevar:z}.Parent_company
WHERE Primary_Offense IS NOT NULL
GROUP BY ${hivevar:z}.Primary_Offense, ${hivevar:x}.sponsor
ORDER BY frequency DESC
LIMIT 5;

-- COMMAND ----------

SELECT DISTINCT ${hivevar:z}.Primary_Offense, ${hivevar:x}.sponsor, COUNT (${hivevar:x}.sponsor) AS Frequency
FROM ${hivevar:x}
FULL JOIN ${hivevar:z}
ON ${hivevar:x}.sponsor=${hivevar:z}.Parent_company
WHERE Primary_Offense IS NOT NULL
GROUP BY ${hivevar:z}.Primary_Offense, ${hivevar:x}.sponsor
ORDER BY frequency DESC
LIMIT 5;
