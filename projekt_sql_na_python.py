#!/usr/bin/env python
# coding: utf-8

# **Barbara Jankowska**
# 
# __Rozwiązanie pracy domowej nr 3__

# ## 1. Przygotowanie danych
# 
# 1. Wykonaj `import` potrzebnych pakietów (oprócz poniżej wymienionych), tak aby umieszczony w tym pliku kod działał.
# :


import numpy as np
import pandas as pd
import sqlite3
import os, os.path


# 2. Wczytaj ramki danych, na których będziesz dalej pracował
# **UWAGA:**
# * Pliki muszą znajdować się w katalogu "travel_stackexchange_com" w tym samym katalogu co ten plik (plik .py).
# * Nazwy tabel muszą być *zgodne z instrukcją zamieszczoną w treści pracy domowej*.
#
#:


# DOPISZ TU ODPOWIEDNI KOD
Posts = pd.read_csv("travel_stackexchange_com/Posts.csv.gz",
compression = 'gzip')
Comments = pd.read_csv("travel_stackexchange_com/Comments.csv.gz",
compression = 'gzip')
PostLinks = pd.read_csv("travel_stackexchange_com/PostLinks.csv.gz",
compression = 'gzip')
Users = pd.read_csv("travel_stackexchange_com/Users.csv.gz",
compression = 'gzip')
Votes = pd.read_csv("travel_stackexchange_com/Votes.csv.gz",
compression = 'gzip')



# 3. Przygotuj bazę danych wykonując poniższą komórkę.

#:


# Ścieżka do pliku z bazą danych ('./' oznacza bieżący katalog, 
# czyli będzie to plik w tym samym katalogu, co ten notebook).

SCIEZKA_BAZY = './pd5_baza.db'  
with sqlite3.connect(SCIEZKA_BAZY) as conn: # połączenie do bazy danych
    # wewnątrz bloku `with` mamy dostępny obiekt połączenia, które jest automatycznie zamykane po jego opuszczeniu.
    Comments.to_sql("Comments", conn, if_exists='replace')  # jeżeli ramka danych już istnieje, to jest nadpisywana.
    Posts.to_sql("Posts", conn, if_exists='replace')
    Users.to_sql("Users", conn, if_exists='replace')
    Votes.to_sql("Votes", conn, if_exists='replace')
    PostLinks.to_sql("PostLinks", conn, if_exists='replace')


# ## 2. Wyniki zapytań SQL
# 
# Wykonaj zapytania sql.


zapytanie_1 = """
SELECT STRFTIME('%Y', CreationDate) AS Year, 
       STRFTIME('%m', CreationDate) AS Month, 
       COUNT(*) AS TotalAccountsCount, 
       AVG(Reputation) AS AverageReputation
FROM Users
GROUP BY Year, Month
"""

zapytanie_2 = """
SELECT Users.DisplayName, Users.Location, Users.Reputation, 
       STRFTIME('%Y-%m-%d', Users.CreationDate) AS CreationDate,
       Answers.TotalCommentCount
FROM (
        SELECT OwnerUserId, SUM(CommentCount) AS TotalCommentCount
        FROM Posts
        WHERE PostTypeId == 2 AND OwnerUserId != ''
        GROUP BY OwnerUserId
     ) AS Answers
JOIN Users ON Users.Id == Answers.OwnerUserId
ORDER BY TotalCommentCount DESC
LIMIT 10
"""


zapytanie_3 = """
SELECT Spam.PostId, UsersPosts.PostTypeId, UsersPosts.Score, 
       UsersPosts.OwnerUserId, UsersPosts.DisplayName,
       UsersPosts.Reputation
FROM (
        SELECT PostId  
        FROM Votes
        WHERE VoteTypeId == 12
     ) AS Spam
JOIN (
        SELECT Posts.Id, Posts.OwnerUserId, Users.DisplayName, 
               Users.Reputation, Posts.PostTypeId, Posts.Score
        FROM Posts JOIN Users
        ON Posts.OwnerUserId = Users.Id
     ) AS UsersPosts 
ON Spam.PostId = UsersPosts.Id
"""


zapytanie_4 = """
SELECT Users.Id, Users.DisplayName, Users.UpVotes, Users.DownVotes, Users.Reputation,
       COUNT(*) AS DuplicatedQuestionsCount
FROM (
        SELECT Duplicated.RelatedPostId, Posts.OwnerUserId
        FROM (
                SELECT PostLinks.RelatedPostId
                FROM PostLinks
                WHERE PostLinks.LinkTypeId == 3
             ) AS Duplicated
        JOIN Posts
        ON Duplicated.RelatedPostId = Posts.Id
     ) AS DuplicatedPosts
JOIN Users ON Users.Id == DuplicatedPosts.OwnerUserId
GROUP BY Users.Id
HAVING DuplicatedQuestionsCount > 100
ORDER BY DuplicatedQuestionsCount DESC
"""

zapytanie_5 = """
SELECT QuestionsAnswers.Id,
       QuestionsAnswers.Title, 
       QuestionsAnswers.Score,
       MAX(Duplicated.Score) AS MaxScoreDuplicated,
       COUNT(*) AS DulicatesCount,
       CASE 
         WHEN QuestionsAnswers.Hour < '06' THEN 'Night'
         WHEN QuestionsAnswers.Hour < '12' THEN 'Morning'
         WHEN QuestionsAnswers.Hour < '18' THEN 'Day'
         ELSE 'Evening'
         END DayTime
FROM (
        SELECT Id, Title, 
               STRFTIME('%H', CreationDate) AS Hour, Score 
        FROM Posts
        WHERE Posts.PostTypeId IN (1, 2)
     ) AS QuestionsAnswers
JOIN (
        SELECT PL3.RelatedPostId, Posts.Score
        FROM (
               SELECT RelatedPostId, PostId
               FROM PostLinks
               WHERE LinkTypeId == 3
             ) AS PL3
        JOIN Posts ON PL3.PostId = Posts.Id
     ) AS Duplicated
ON QuestionsAnswers.Id = Duplicated.RelatedPostId
GROUP BY QuestionsAnswers.Id
ORDER By DulicatesCount DESC
"""

# Poniższy blok with wykonuje wszystkie 5 zapytań;
# Wyniki umieszcza w zmiennych sql_i.
with sqlite3.connect(SCIEZKA_BAZY) as conn:
    sql_1 = pd.read_sql_query(zapytanie_1, conn)
    sql_2 = pd.read_sql_query(zapytanie_2, conn)
    sql_3 = pd.read_sql_query(zapytanie_3, conn)
    sql_4 = pd.read_sql_query(zapytanie_4, conn)
    sql_5 = pd.read_sql_query(zapytanie_5, conn)



# ## 3. Wyniki zapytań SQL odtworzone przy użyciu metod pakietu Pandas.
# 
# Wynikowa ramka danych do zapytania 1 popwinna nazwyać się `pandas_1`, do drugiego `pandas_2` itd.

# ### Zadanie 1


try:
    Users['Year'] = Users['CreationDate'].str[:4]
    Users['Month'] = Users['CreationDate'].str[5:7]
    pandas_1 = Users.groupby(['Year', 'Month'], as_index=False).agg(
        TotalAccountsCount=('Id', 'size'),
        AverageReputation=('Reputation', 'mean'))

    print(pandas_1.equals(sql_1))

except Exception as e:
    print("Zad. 1: niepoprawny wynik.")
    print(e)


# ### Zadanie 2


try:
    Answers = Posts[(Posts['PostTypeId'] == 2) & (Posts['OwnerUserId'] != '')].groupby('OwnerUserId')['CommentCount'].agg(
        TotalCommentCount='sum').reset_index()

    pandas_2 = pd.merge(Answers, Users, left_on='OwnerUserId', right_on='Id')[['DisplayName', 'Location',
    'Reputation', 'CreationDate', 'TotalCommentCount']]
    pandas_2['CreationDate'] = pandas_2['CreationDate'].str[:10]
    pandas_2 = pandas_2.sort_values(by='TotalCommentCount', ascending=False).reset_index(drop=True).head(10)

    # sprawdzenie równoważności wyników
    print(pandas_2.equals(sql_2))

except Exception as e:
    print("Zad. 2: niepoprawny wynik.")
    print(e)


# ### Zadanie 3


try:
    Spam = Votes[Votes['VoteTypeId'] == 12][['PostId']]
    UsersPosts = pd.merge(Posts, Users, left_on='OwnerUserId', right_on='Id')[['Id_x', 'OwnerUserId',
    'DisplayName', 'Reputation', 'PostTypeId', 'Score']]
    pandas_3 = pd.merge(Spam, UsersPosts, left_on='PostId', right_on='Id_x')
    pandas_3 = pandas_3[['PostId', 'PostTypeId', 'Score', 'OwnerUserId', 'DisplayName', 'Reputation']]

    # sprawdzenie równoważności wyników
    print(pandas_3.equals(sql_3))

except Exception as e:
    print("Zad. 3: niepoprawny wynik.")
    print(e)


# ### Zadanie 4

Duplicated = PostLinks[PostLinks['LinkTypeId'] == 3][['RelatedPostId']]
DuplicatedPosts = pd.merge(Duplicated, Posts, left_on='RelatedPostId', right_on='Id')[['RelatedPostId', 'OwnerUserId']]

pandas_4 = pd.merge(Users, DuplicatedPosts, left_on='Id', right_on='OwnerUserId')
DQC = pandas_4.groupby('Id')['Id'].agg(DuplicatedQuestionsCount='size').reset_index()
pandas_4 = pd.merge(pandas_4, DQC, on='Id')[['Id', 'DisplayName', 'UpVotes', 'DownVotes', 'Reputation', 'DuplicatedQuestionsCount']]
pandas_4 = pandas_4[pandas_4['DuplicatedQuestionsCount'] > 100].sort_values(by='DuplicatedQuestionsCount',
        ascending=False).drop_duplicates().reset_index(drop=True)

try:
    uplicated = PostLinks[PostLinks['LinkTypeId'] == 3][['RelatedPostId']]
    DuplicatedPosts = pd.merge(Duplicated, Posts, left_on='RelatedPostId', right_on='Id')[
        ['RelatedPostId', 'OwnerUserId']]

    pandas_4 = pd.merge(Users, DuplicatedPosts, left_on='Id', right_on='OwnerUserId')
    DQC = pandas_4.groupby('Id')['Id'].agg(DuplicatedQuestionsCount='size').reset_index()
    pandas_4 = pd.merge(pandas_4, DQC, on='Id')[['Id', 'DisplayName', 'UpVotes', 'DownVotes', 'Reputation', 'DuplicatedQuestionsCount']]
    pandas_4 = pandas_4[pandas_4['DuplicatedQuestionsCount'] > 100].sort_values(by='DuplicatedQuestionsCount',
                ascending=False).drop_duplicates().reset_index(drop=True)
    # sprawdzenie równoważności wyników
    print(pandas_4.equals(sql_4))

except Exception as e:
    print("Zad. 4: niepoprawny wynik.")
    print(e)


# ### Zadanie 5

QuestionsAnswers = Posts[(Posts['PostTypeId'] == 1) | (Posts['PostTypeId'] == 2)]
QuestionsAnswers['Hour'] = QuestionsAnswers['CreationDate'].str[11:13]
QuestionsAnswers = QuestionsAnswers[['Id', 'Title', 'Hour', 'Score']]
PL3 = PostLinks[PostLinks['LinkTypeId'] == 3][['RelatedPostId', 'PostId']]
Duplicated = pd.merge(Posts, PL3, left_on='Id', right_on='PostId')
pandas_5 = pd.merge(QuestionsAnswers, Duplicated, left_on='Id', right_on='RelatedPostId')

DC_MAX = pandas_5.groupby('Id_x').agg(
        DuplicatesCount=('Id_x', 'size'),
        MaxScoreDuplicated=('Score_y', 'max'))
pandas_5 = pd.merge(pandas_5, DC_MAX, on='Id_x')
pandas_5 = pandas_5[['Id_x', 'Title_x', 'Score_x', 'DuplicatesCount', 'MaxScoreDuplicated']].sort_values(by='DuplicatesCount',
        ascending=False).reset_index(drop=True)


try:
    QuestionsAnswers = Posts[(Posts['PostTypeId'] == 1) | (Posts['PostTypeId'] == 2)]
    QuestionsAnswers['Hour'] = QuestionsAnswers['CreationDate'].str[11:13]
    QuestionsAnswers = QuestionsAnswers[['Id', 'Title', 'Hour', 'Score']]
    PL3 = PostLinks[PostLinks['LinkTypeId'] == 3][['RelatedPostId', 'PostId']]
    Duplicated = pd.merge(Posts, PL3, left_on='Id', right_on='PostId')
    pandas_5 = pd.merge(QuestionsAnswers, Duplicated, left_on='Id', right_on='RelatedPostId')

    DC_MAX = pandas_5.groupby('Id_x').agg(
        DuplicatesCount=('Id_x', 'size'),
        MaxScoreDuplicated=('Score_y', 'max'))
    pandas_5 = pd.merge(pandas_5, DC_MAX, on='Id_x')
    pandas_5 = pandas_5[['Id_x', 'Title_x', 'Score_x', 'DuplicatesCount', 'MaxScoreDuplicated']].sort_values(
        by='DuplicatesCount',
        ascending=False).reset_index(drop=True)

    #nieskońćzone - nie ma kolumny DayTime

    # sprawdzenie równoważności wyników
    print(pandas_5.equals(sql_5))

except Exception as e:
    print("Zad. 5: niepoprawny wynik.")
    print(e)

