# StackExchangeDataUsingPigHive
ETL using Pig, Calculating TFIDF using Hive

Task 1: Data Acquisition

StackExchange is a question-and-answer platform on topics in diverse field. These questions and answers of all the posts can be retreived using sql queries on  StackExchangeDataExplorer avaliable at: https://data.stackexchange.com/stackoverflow/query/new. First task is to query top 2,00,000 posts, data explorer will only allow 50,000 records to be downloaded at once. 2,00,000 posts were fethced four times using sql queries with First query: 1.	SELECT top 50000 * FROM posts WHERE posts.ViewCount > 100000 ORDER BY posts.ViewCount DESC; followed by four three other queries.

Task 2: ETL

Extracting, transforming and loading data is easier in Pig when compared to hive. Development time when using Pig is much lesser than MapReduce. 
Body column was tricky to clean, I initially removed all html tags, commas in Body,Title and Tags columns. When the data was loaded to hive, there were hardly few valid data with all the other rows with NULL values. It was because the line breaks were not removed due to which the data was not in proper format. After removing the line breaks, the data was ready to be used in HIVE.

Task 3: Hive Queries

The output of Pig was stored in HDFS. A table in Hive was first created, which was then loaded with the pig output that was stored on HDFS.
Three simple queries were performed to fetch top ten posts by score,top ten users by posts score and the number of distinct users who used the word 'Hadoop' in one their posts.

Task 4: TFIDF Calculation

TFIDF is calculated on Body column. The result of the second query in Task 3 consists of top ten users by posts score, which was was loaded to a new table(For eg: A) in hive. Another new table(For eg:B) was created to fetch Body of all the top ten users by using OwnerUserId from table A; TFIDF was then calculated based on this table. The final query fetches top ten terms per user(a total of 100 records).

The entire process involved Pig and Hive only. It was all performed on GCP.

TFIDF was implemented by referring: https://github.com/harshalchaudhari35/StackExchange_DataProcessing/blob/master/code/pig_script.pig
