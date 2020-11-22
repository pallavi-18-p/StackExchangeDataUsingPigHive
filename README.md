# StackExchangeDataUsingPigHive
ETL using Pig, Calculating TFIDF using Hive

Task 1: Data Acquisition

StackExchange is a question-and-answer platform on topics across diverse fields. These questions and answers of all the posts can be retreived using sql queries on  StackExchangeDataExplorer avaliable at: https://data.stackexchange.com/stackoverflow/query/new. The first task is to query top 2,00,000 posts. Data explorer will only allow 50,000 records to be downloaded at once. The 2,00,000 posts were fetched four times using sql queries with the first query being: 1.	SELECT top 50000 * FROM posts WHERE posts.ViewCount > 100000 ORDER BY posts.ViewCount DESC; followed by four other queries. Last set was used for testing purposes.

1. SELECT top 50000 * FROM posts WHERE posts.ViewCount <=112523 ORDER BY posts.ViewCount DESC;
2. SELECT top 50000 * FROM posts WHERE posts.ViewCount <=66244 ORDER BY posts.ViewCount DESC;
3. SELECT top 50000 * FROM posts WHERE posts.ViewCount <=47290 ORDER BY posts.ViewCount DESC;
4. SELECT top 50000 * FROM posts WHERE posts.ViewCount <=36780 and posts.ViewCount >= 36751 ORDER BY posts.ViewCount DESC;

Task 2: ETL

Extracting, transforming and loading data is easier in Pig when compared to MapReduce. Development time when using Pig is much lesser than MapReduce. 
Body column was tricky to clean, I initially removed all html tags, commas in Body, Title and Tags columns. When the data was loaded to Hive, there was hardly any valid data with all the other rows containing NULL values. It was because the line breaks were not removed due to which the data was not in proper format. After removing the line breaks, the data was ready to be used in Hive.

Task 3: Hive Queries

The output of Pig was stored in HDFS. A table in Hive was first created, which was then loaded with the Pig output that was stored on HDFS.
Three simple queries were performed to fetch top ten posts by score, top ten users by posts score, and the number of distinct users who used the word 'Hadoop' in one of their posts.

Task 4: TFIDF Calculation

TFIDF is calculated on the Body column. The result of the second query in Task 3 consists of top ten users by posts score, which was loaded to a new table (For eg: A) in Hive. Another new table (For eg: B) was created to fetch the Body of all the top ten users by using OwnerUserId from table A; TFIDF was then calculated based on this table. The final query fetches top ten terms per user (a total of 100 records).

The entire process involved Pig and Hive only. It was all performed on GCP.

References

1. Stack Overflow: https://stackoverflow.com/
2. TFIDF was implemented by referring: https://github.com/harshalchaudhari35/StackExchange_DataProcessing/blob/master/code/pig_script.pig
3. Hive: https://github.com/arunabellgutteramesh/PigHiveOnStackExchangeData/blob/master/code/Querying/hiveQueries.sql

