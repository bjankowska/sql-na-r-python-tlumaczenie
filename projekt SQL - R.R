
# Wczytanie danych oraz pakietow.

library(sqldf)
library(dplyr)
library(data.table)
library(compare)
library(microbenchmark)

# Oryginalne, pełne ramki danych
#Posts <- read.csv("travel_stackexchange_com/Posts.csv.gz")
#PostLinks <- read.csv("travel_stackexchange_com/PostLinks.csv.gz")
#Comments <- read.csv("travel_stackexchange_com/Comments.csv.gz")
#Users <- read.csv("travel_stackexchange_com/Users.csv.gz")
#Votes <- read.csv("travel_stackexchange_com/Votes.csv.gz")

# Próbki
Posts <- read.csv("travel_stackexchange_samples/Posts_sample.csv.gz")
PostLinks <- read.csv("travel_stackexchange_samples/PostLinks_sample.csv.gz")
Comments <- read.csv("travel_stackexchange_samples/Comments_sample.csv.gz")
Users <- read.csv("travel_stackexchange_samples/Users_sample.csv.gz")
Votes <- read.csv("travel_stackexchange_samples/Votes_sample.csv.gz")


# -----------------------------------------------------------------------------
# Zadanie 1
# -----------------------------------------------------------------------------

sql_1 <- function(Users){
    sqldf("SELECT STRFTIME('%Y', CreationDate) AS Year,
                  STRFTIME('%m', CreationDate) AS Month,
                  COUNT(*) AS TotalAccountsCount,
                  AVG(Reputation) AS AverageReputation
          FROM Users
          GROUP BY Year, Month")
  #Funkcja ta zwraca nam ramkę danych, która pokazuje nam w kolejnych wierszach kolejne miesiące (z kolejnych lat).
  #W kolumnie TotalAccountsCount pokazuje liczbę utworzonnych kont w danym miesiącu w danym roku.
  #W kolumnie Average Reputation pokazuje nam średnią wartość Reputation dla kont utworzonych w danym miesiącu w danym roku.
  #Wszystko to oczywiście z ramki danych Users
}

base_1 <- function(Users){
  #Dodaje do ramki danych Users kolumny year i month (wyciągam je za pomocą funkcji substr z kolumny CreationDate)
  Users$Year <- substr(Users$CreationDate, 1, 4)
  Users$Month <- substr(Users$CreationDate, 6, 7)
  #Liczę liczbę utworzonych kont utworzonych w danych miesiącach używając funkcji aggregate
  df1 <- aggregate(Users$Reputation, Users[c("Month", "Year")], length)
  #Liczę śrędnią wartość Reputation dla poszczególnych miesięcy używając funkcji aggregate
  df2 <- aggregate(Users$Reputation, Users[c("Month", "Year")], mean)
  #Łączę ramki df1 i df2 za pomocą funkcji cbind
  base1 <- cbind(df1, df2["x"])
  #Odpowiednio zmieniam nazwy kolumn
  colnames(base1) <- c("Month", "Year", "TotalAccountsCount", "AverageReputation")
  base1
}

dplyr_1 <- function(Users){
  Users %>%
    #Dodaje do ramki Users kolumny Year i Month (wyciągam je znowu z CreationDate za pomocą funkcji substr)
    mutate(Year = substr(CreationDate, 1, 4), Month = substr(CreationDate, 6, 7)) %>%
    #Grupuję względem kolumn Year i Month
    group_by(Year, Month) %>%
    #Liczę średnią Reputation w każdym miesiącu za pomocą funkcji mean - dostaję kolumnę AverageReputation
    #i ilość utworzonych kont w każdym miesiącu za pomocą n() - dostaję kolumnę TotalAccountsCount
    summarise(AverageReputation = mean(Reputation), TotalAccountsCount = n())
}

table_1 <- function(Users){
  #Zmieniam data.frame na data.table
  UsersDT <- as.data.table(Users)
  #Dodaję kolumny Year i Month (jak wcześniej za pomocą substr)
  UsersDT[, ':=' (Year = substr(Users$CreationDate, 1, 4), Month = substr(Users$CreationDate, 6, 7))]
  #Liczę TotalAccountsCount (za pomocą funkcji .N) oraz średnią Reputation grupując według lat i miesięcy
  wynik <- UsersDT[, .(TotalAccountsCount = .N, AverageReputation = mean(Reputation)), by = .(Year, Month)]
  #Zmieniam wynik z data.table na data.frame
  as.data.frame(wynik)
}

# Sprawdzenie rownowaznosci wynikow 
compare(sql_1(Users), base_1(Users), allowAll = TRUE)
compare(sql_1(Users), dplyr_1(Users), allowAll = TRUE)
compare(sql_1(Users), table_1(Users), allowAll = TRUE)

# Porowanie czasow wykonania
microbenchmark(
  sqldf = sql_1(Users),
  base = base_1(Users),
  dplyr = dplyr_1(Users),
  data.table = table_1(Users),
  times = 5L
)


# -----------------------------------------------------------------------------#
# Zadanie 2
# -----------------------------------------------------------------------------#

sql_2 <- function(Posts, Users){
  sqldf("SELECT Users.DisplayName, Users.Location, Users.Reputation,
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
          LIMIT 10")
  #Answers - ramka danych z kolumnami: OwnerUserId i TotalComment Count, pokazuje nam Id użytkowników
  #i sumę komentarzy pod ich postami, przy czym nie liczą się użytkownicy, którzy nie mają nazwy
  #oraz liczą się tylko posty o PostTypeId równym 2.
  #Wynikowa ramka pokazuje 10 użytkowników o największej ilości komentarzy pod postami (TotalCommentCount) 
  #(w kolejności malejącej). W kolumnach są ich dane: DisplayName, Location, Reputation, CreationDate 
  #(zmienione na sam rok-miesiąc-dzień) i TotalCommentCount (otrzymaliśmy je dzięki połączeniu ramki 
  #Answers z ramką Users)
}



base_2 <- function(Posts, Users){
  #wybieram te wiersze z ramki Posts, dla których PostTypeId wynosi 2 i OwnerUserId jest różne od ''
  Posts2 <- Posts[which(Posts$PostTypeId == 2 & Posts$OwnerUserId != ''), ]
  #Tworzę ramkę Answers za pomocą funkcji aggregate
  Answers <- aggregate(Posts2$CommentCount, Posts2['OwnerUserId'], sum)
  colnames(Answers) <- c('OwnerUserId', 'TotalCommentCount')
  #Zmieniam date, żeby był sam rok, miesiąc i dzień za pomocą funkcji substr
  Users$CreationDate <- substr(Users$CreationDate, 1, 10)
  #Wybieram odpowiednie kolumny z ramki Users
  Users2 <- Users[ , c('Id', 'DisplayName', 'Location', 'Reputation', 'CreationDate')]
  #Łącze ramkę Users2 i Answers i porządkuje malejąco względem TotalCommentCOunt
  base2 <- merge(x=Users2, y=Answers, by.x='Id', by.y='OwnerUserId')
  base2 <- base2[order(base2$TotalCommentCount, decreasing=TRUE), ]
  #Wybieram pierwsze 10 wierszy
  base2[1:10, ]
}

dplyr_2 <- function(Posts, Users){
  Posts %>%
    #Wybieram te wiersze z ramki Posts, dla których PostTypeId wynosi 2 i OwnerUserId jest różne od ''
    filter(PostTypeId == 2, OwnerUserId != '') %>%
    #Grupuje według OwnerUserId i liczę TotalCommentCount za pomocą summarize()
    group_by(OwnerUserId) %>%
    summarize(TotalCommentCount = sum(CommentCount)) %>%
    #Przyłączam odpowiednio ramkę Users
    inner_join(Users, by = c('OwnerUserId' = 'Id')) %>%
    #Zmieniam Creation Date tak aby był sam rok, miesiąc i dzień
    mutate(CreationDate = substr(CreationDate, 1, 10)) %>%
    #Wybieram odpowiednie kolumny
    select(DisplayName, Location, Reputation, CreationDate, TotalCommentCount) %>%
    #Porządkuję malejąco według TotalCommentCount
    arrange(desc(TotalCommentCount)) %>%
    #Wybieram pierwsze 10 wierszy
    head(10)
}

table_2 <- function(Posts, Users){
  #Zmieniam data.fram na data.table (Users) i odpowiednio zmieniam CreationDate
  UsersDT <- as.data.table(Users)
  UsersDT[, ':=' (CreationDate = substr(CreationDate, 1, 10))]
  #Zmieniam data.frame na data.table (Posts)
  PostsDT <- as.data.table(Posts)
  #Tworzę Answers
  Answers <- PostsDT[PostTypeId == 2 & OwnerUserId != '', .(TotalCommentCount = sum(CommentCount)), by = .(OwnerUserId)]
  #Łączę Answers z UsersDT, kolejność od największej ilość komentarzy wybieramy pierwsze 10 wierszy i podane kolumny
  wynik <- Answers[UsersDT, on = .(OwnerUserId = Id), nomatch=NULL][order(-TotalCommentCount)][1:10, .(DisplayName, Location, Reputation, CreationDate, TotalCommentCount)]
  #Zmieniam wynik na data.frame
  as.data.frame(wynik)
}


# Sprawdzenie rownowaznosci wynikow
compare(sql_2(Posts, Users), base_2(Posts, Users), allowAll = TRUE)
compare(sql_2(Posts, Users), dplyr_2(Posts, Users), allowAll = TRUE)
compare(sql_2(Posts, Users), table_2(Posts, Users), allowAll = TRUE)

# Porowanie czasow wykonania
microbenchmark(
  sqldf = sql_2(Posts, Users),
  base = base_2(Posts, Users),
  dplyr = dplyr_2(Posts, Users),
  data.table = table_2(Posts, Users),
  times = 5L
)

# -----------------------------------------------------------------------------#
# Zadanie 3
# -----------------------------------------------------------------------------#

sql_3 <- function(Posts, Users, Votes){
    sqldf("SELECT Spam.PostId, UsersPosts.PostTypeId, UsersPosts.Score,
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
          ON Spam.PostId = UsersPosts.Id")
  #Spam - ramka danych z jedną kolumną pokazująca PostId postów o VoteTypeId równym 12.
  #UsersPosts - połączona ramka danych Posts z Users, pokazuje dane o użytkownikach (DisplayName, 
  #Reputation, Id użytkowników) i utworzonych przez nich postach (Id postów, PostTypeId, Score).
  #Wynikowa ramka powstaje w wyniku połączenia ramki Spam z UsersPosts i pokazuje dane tych postów
  #(i użytkowników, którzy je utworzyli), których Id znajdowały się w ramce Spam.
  #Dane: postów - Id postów, PostTypeId, użytkowników - Score, Id użytkowników, DisplayName, Reputation
}


base_3 <- function(Posts, Users, Votes){
  #Tworzę ramkę danych Spam
  Spam <- Votes[which(Votes$VoteTypeId == 12), ]
  Spam2 <- Spam['PostId']
  #Tworzę ramkę danych UsersPosts
  Posts2 <- Posts[ , c('Id', 'OwnerUserId', 'PostTypeId', 'Score')]
  Users2 <- Users[ , c('Id', 'DisplayName', 'Reputation')]
  UsersPosts <- merge(x=Posts2, y=Users2, by.x='OwnerUserId', by.y='Id')
  #Łączę ramki Spam2 i UsersPosts
  merge(x=Spam2, y=UsersPosts, by.x='PostId', by.y='Id')
}

dplyr_3 <- function(Posts, Users, Votes){
  #Tworzę ramkę danych UsersPosts
  UsersPosts <- Posts %>%
    inner_join(Users, by = c('OwnerUserId' = 'Id')) %>%
    select(Id, OwnerUserId, DisplayName, Reputation, PostTypeId, Score)
  #Tworzę ramkę Spam
  Votes %>%
    filter(VoteTypeId == 12) %>%
    select(PostId) %>%
    #Łączę ją od razu z UsersPosts
    inner_join(UsersPosts, by = c('PostId' = 'Id'))
}

table_3 <- function(Posts, Users, Votes){
  #Zmieniam data.frame na data.table
  VotesDT <- as.data.table(Votes)
  PostsDT <- as.data.table(Posts)
  UsersDT <- as.data.table(Users)
  #Tworzę Spam
  Spam <- VotesDT[VoteTypeId == 12, .(PostId)]
  #Tworzę UsersPosts
  UsersPosts <- PostsDT[UsersDT, on = .(OwnerUserId = Id), nomatch=NULL][, .(Id, OwnerUserId, DisplayName, Reputation, PostTypeId, Score)]
  #Łączę Spam i UsersPosts
  wynik <- Spam[UsersPosts, on = .(PostId = Id), nomatch = NULL]
  #Zmieniam data.table na data.frame
  as.data.frame(wynik)
}


# Sprawdzenie rownowaznosci wynikow
compare(sql_3(Posts, Users, Votes), base_3(Posts, Users, Votes), allowAll = TRUE)
compare(sql_3(Posts, Users, Votes), dplyr_3(Posts, Users, Votes), allowAll = TRUE)
compare(sql_3(Posts, Users, Votes), table_3(Posts, Users, Votes), allowAll = TRUE)

# Porowanie czasow wykonania
microbenchmark(
  sqldf = sql_3(Posts, Users, Votes),
  base = base_3(Posts, Users, Votes),
  dplyr = dplyr_3(Posts, Users, Votes),
  data.table = table_3(Posts, Users, Votes),
  times = 5L
)

# -----------------------------------------------------------------------------#
# Zadanie  4
# -----------------------------------------------------------------------------#

sql_4 <- function(Posts, Users, PostLinks){
    sqldf("SELECT Users.Id, Users.DisplayName, Users.UpVotes, Users.DownVotes, Users.Reputation,
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
")
  #Duplicated - ramka danych z jedną kolumną pokazująca RelatedPostId dla tych postów, których
  #LinkTypeId jest równy 3
  #DuplicatedPosts - powstaje w wyniku połączenia Duplicated z Users RelatedPostId, pokazuje
  #OwnerUserId dla postów, których Id są w kolumnie RelatedPostId w ramce Duplicated
  #Ostateczna ramka powstaje przez połączenie DuplicatedPosts z Users oraz grupowanie wzgledem Id użytkowników
  #Pokazuje nam dane użytkowników: Id, DisplayName, UpVotes, DownVotes, Reputation oraz 
  #DuplicatedQuestionsCount, czyli ilość postów znajdujących się w ramce DuplicatedPosts
  #Bierzemy te wiersze w których DuplicatedQuestionsCount jest większe od 100 i porządkujemy 
  #malejąco względem DuplicatedQuestionsCount
}

base_4 <- function(Posts, Users, PostLinks){
  #Tworzę ramkę danych Duplicated
  Duplicated <- as.data.frame(PostLinks[which(PostLinks$LinkTypeId == 3), 'RelatedPostId'])
  colnames(Duplicated) <- 'RelatedPostId'
  #Tworzę ramkę danych DP(DuplicatedPosts)
  DP <- merge(x=Posts, y=Duplicated, by.x='Id', by.y='RelatedPostId')
  DP <- DP[ , c('Id', 'OwnerUserId')]
  colnames(DP) <- c('RelatedPostId', 'OwnerUserId')
  #Liczę DuplicatedQuestionsCount za pomocą funkcji aggregate
  DuplicatedPosts <- aggregate(DP$RelatedPostId, DP['OwnerUserId'], length)
  colnames(DuplicatedPosts) <- c('OwnerUserId', 'DuplicatedQuestionsCount')
  #Łączę Users z DuplicatedPosts
  Users2 <- merge(x=Users, y=DuplicatedPosts, by.x='Id', by.y='OwnerUserId')
  Users2 <- Users2[ , c('Id', 'DisplayName', 'UpVotes', 'DownVotes', 'Reputation', 'DuplicatedQuestionsCount')]
  #Wybieram wiersze w których DuplicatedQuestionsCount jest większe od 100 i porządkuję
  #malejąco względem DuplicatedQuestionsCount
  base4 <- Users2[which(Users2$DuplicatedQuestionsCount > 100), ]
  base4 <- base4[order(base4$DuplicatedQuestionsCount, decreasing=TRUE), ]
  base4
}

dplyr_4 <- function(Posts, Users, PostLinks){
  PostLinks %>%
    #Wybieram te wiersze z PostLinks, dla których LinkTypeId jest równy 3, wybieram kolumne RelatedPostId
    filter(LinkTypeId == 3) %>%
    select(RelatedPostId) %>%
    #Łączę odpowiednio z Posts
    inner_join(Posts, by = c('RelatedPostId' = 'Id')) %>%
    select(RelatedPostId, OwnerUserId) %>%
    #Łączę odpowiednio z Users
    inner_join(Users, ., by = c('Id' = 'OwnerUserId')) %>%
    #Grupuję według Id, liczę DuplicatedQuestionsCount i wybieram pozostałe kolumny
    group_by(Id) %>%
    summarize(DuplicatedQuestionsCount = n(), Id, DisplayName, UpVotes, DownVotes, Reputation) %>%
    #Używam funkcji distinct aby wiersze się nie powtarzały
    distinct(Id, DisplayName, UpVotes, DownVotes, Reputation, DuplicatedQuestionsCount) %>%
    #Wybieram wiersze w których DuplicatedQuestionsCount jest większe od 100 i porządkuję
    #malejąco względem DuplicatedQuestionsCount
    filter(DuplicatedQuestionsCount > 100) %>%
    arrange(desc(DuplicatedQuestionsCount))
}

table_4 <- function(Posts, Users, PostLinks){
  #Zamieniam data.frame na data.table
  PostLinksDT <- as.data.table(PostLinks)
  PostsDT <- as.data.table(Posts)
  UsersDT <- as.data.table(Users)
  #Tworzę Duplicated i DuplicatedPosts
  Duplicated <- PostLinksDT[LinkTypeId == 3, .(RelatedPostId)]
  DuplicatedPosts <- Duplicated[PostsDT, on = .(RelatedPostId = Id), nomatch=NULL]
  #Łączę DuplicatedPosts z UsersDT, wybieram odpowiednie kolumny, liczę DuplicatedQuestionsCount 
  #za pomocą .N, wybieram wiersze dla których DuplicatedQuestionsCount > 100 oraz kolejność
  #od największej ilości DuplicatedQuestionsCount
  wynik <- UsersDT[DuplicatedPosts, on = .(Id = OwnerUserId)][, .(DisplayName, UpVotes, DownVotes, Reputation, DuplicatedQuestionsCount = .N), by = .(Id)][DuplicatedQuestionsCount > 100, ]
  #Używam funkcji unique żeby usunąć powtarzające się wiersze oraz funkcji na.omit aby usunąć wiersz z NA (nienznaną wartością)
  wynik <- unique(wynik)
  wynik <- na.omit(wynik)
  #Zamieniam na data.frame
  as.data.frame(wynik)
}

# Sprawdzenie rownowaznosci wynikow
compare(sql_4(Posts, Users, PostLinks), base_4(Posts, Users, PostLinks), allowAll = TRUE)
compare(sql_4(Posts, Users, PostLinks), dplyr_4(Posts, Users, PostLinks), allowAll = TRUE)
compare(sql_4(Posts, Users, PostLinks), table_4(Posts, Users, PostLinks), allowAll = TRUE)

# Porowanie czasow wykonania
microbenchmark(
  sqldf = sql_4(Posts, Users, PostLinks),
  base = base_4(Posts, Users, PostLinks),
  dplyr = dplyr_4(Posts, Users, PostLinks),
  data.table = table_4(Posts, Users, PostLinks),
  times = 5L
)

# -----------------------------------------------------------------------------#
# Zadanie 5
# -----------------------------------------------------------------------------#

sql_5 <- function(Posts, PostLinks){
    sqldf("SELECT QuestionsAnswers.Id,
                  QuestionsAnswers.Title,
                  QuestionsAnswers.Score,
                  MAX(Duplicated.Score) AS MaxScoreDuplicated,
                  COUNT(*) AS DuplicatesCount,
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
          ORDER By DuplicatesCount DESC
")
  #QuestionsAnswers - pokazuje Id i tytuł postów, których PostTypeId jest równy 1 lub 2
  #PL3 - pokazuje RelatedPostId i PostId tych postów, których LinkTypeId jest równy 3
  #Duplicated - pokazuje RelatedPostId i Score tych postów, których LinkTypeId jest równy 3
  #Wynikowa ramka powstaje przez połączenie QuestionAnswers z Duplicated,
  #Pokazuje dane postów: Id, Title, Score, DayTime (czas dnia kiedy zostały utworzone),
  #MaxScoreDuplicated czyli maksymalną wartość Score z ramki Duplicated (dla posta o danym Id) oraz
  #DuplicatesCount czyli ile razy post o tym Id pojawiał się w ramce Duplicated
  #Ramka uporządkowana według DuplicatesCount malejąco
  
}

base_5 <- function(Posts, PostLinks){
  #Tworzę w Posts kolumnę Hour
  Posts$Hour <- substr(Posts$CreationDate, 12, 13)
  #Tworzę ramkę QuestionsAnswers
  QuestionsAnswers <- Posts[which(Posts$PostTypeId == 1 | Posts$PostTypeId == 2) , c('Id', 'Title', 'Hour', 'Score')]
  #Tworzę ramkę PL3
  PL3 <- PostLinks[which(PostLinks$LinkTypeId == 3) , c('RelatedPostId', 'PostId')]
  #Tworzę ramkę Duplicated
  Duplicated <- merge(x=Posts, y=PL3, by.x='Id', by.y='PostId')[ , c('RelatedPostId', 'Score')]
  #Tworzę ramkę Duplicatedmax i DuplicatesCount (za pomocą aggregate) i je łączę
  Duplicatedmax <- aggregate(Duplicated$Score, Duplicated['RelatedPostId'], max)
  DuplicatesCount <- aggregate(Duplicated$RelatedPostId, Duplicated['RelatedPostId'], length)
  x <- merge(x=Duplicatedmax, y=DuplicatesCount, by='RelatedPostId')
  #Łączę QuestionAnswers i x (połączone Duplicatedmax i DuplicatesCount)
  base5 <- merge(x=QuestionsAnswers, y=x, by.x='Id', by.y='RelatedPostId')
  #Zmieniam nazwy kolumn (w tym Hour na Daytime)
  colnames(base5) <- c('Id', 'Title', 'DayTime', 'Score', 'MaxScoreDuplicated', 'DuplicatesCount')
  #Odpowiednio zmieniam DayTime, aby zamiast godziy pokazywała część dnia
  base5[which(as.integer(base5$DayTime) < 6), 'DayTime'] <- 'Night'
  base5[which(as.integer(base5$DayTime) >= 6 & as.integer(base5$DayTime) < 12), 'DayTime'] <- 'Morning'
  base5[which(as.integer(base5$DayTime) >= 12 & as.integer(base5$DayTime) < 18), 'DayTime'] <- 'Day'
  base5[which(as.integer(base5$DayTime) >= 18), 'DayTime'] <- 'Evening'
  #Porządkuję malejąco względem DuplicatesCount
  base5 <- base5[order(base5$DuplicatesCount, decreasing = TRUE), ]
  base5
}

dplyr_5 <- function(Posts, PostLinks){
  #Tworzę Duplicated
  Duplicated <- PostLinks %>%
    filter(LinkTypeId == 3) %>%  #Wybieram te wiersze z PostsLinks, dla których LinkTypeId to 3 (ramka PL3)
    select(RelatedPostId, PostId) %>%
    #Łączę odpowiednio z Posts
    inner_join(Posts, by = c('PostId' = 'Id'))
  #Wybieram te wiersze z Posts, dla których PostTypeId jest równy 1 lub 2 (QuestionsAnswers)
  Posts %>%
    filter(PostTypeId == 1 | PostTypeId == 2) %>%
    #Dodaję kolumnę Hour
    mutate(Hour = substr(CreationDate, 12, 13)) %>%
    #Wybieram odpowiedie kolumny
    select(Id, Title, Hour, Score) %>%
    #Łączę odpowiednio z Duplicated
    inner_join(Duplicated, by = c('Id' = 'RelatedPostId')) %>%
    #Dodaję kolumnę DayTime, korzystam z funkcji case_when z pakietu dplyr
    mutate(DayTime = case_when(
      as.integer(Hour) < 6 ~ 'Night',
      as.integer(Hour) < 12 ~ 'Morning',
      as.integer(Hour) < 18 ~ 'Day',
      .default = 'Evening'
    )) %>%
    #Grupuję według Id, liczę MaxScoreDuplicated i DuplicatesCount oraz wybieram pozostałe kolumny 
    group_by(Id) %>%
    summarize(MaxScoreDuplicated = max(Score.y), DuplicatesCount = n(), Id, Title = Title.x, Score = Score.x, DayTime) %>%
    #Używam distinct aby wiersze się nie powtarzały
    distinct() %>%
    #Porządkuję malejąco według DuplicatesCount
    arrange(desc(DuplicatesCount))
}

table_5 <- function(Posts, PostLinks){
  #Zmieniam data.frame na data.table
  PostLinksDT <- as.data.table(PostLinks)
  PostsDT <- as.data.table(Posts)
  #Tworzę Duplicated (tworzę PL3 i łączę odpowiednio z PostsDT)
  Duplicated <- PostLinksDT[LinkTypeId == 3, .(RelatedPostId, PostId)][PostsDT, on = .(PostId = Id), nomatch = NULL]
  #Dodaję kolumnę Hour w PostsDT
  PostsDT[, ':=' (Hour = substr(CreationDate, 12, 13))]
  #Dodaję kolumnę DayTime w PostsDT za pomocą funkcji fcase z pakietu data.table
  PostsDT[, ':=' (DayTime = fcase(
    as.integer(Hour) < 6, 'Night',
    as.integer(Hour) < 12, 'Morning',
    as.integer(Hour) < 18, 'Day',
    default = 'Evening'
  ))]
  #tworzę QuestionsAnswers, łączę odpowiednio z Duplicated, wybieram odpowiednie kolumny,
  #liczę MaxScoreDuplicated i DuplicatesCount, kolejność malejąca według DuplicatesCount
  wynik <-PostsDT[PostTypeId == 1 | PostTypeId == 2, .(Id, Title, DayTime, Score)][Duplicated, 
  on = .(Id = RelatedPostId), nomatch = NULL][, .(Id, Title, Score, MaxScoreDuplicated = max(i.Score), 
  DuplicatesCount = .N, DayTime), by = .(Id)][order(-DuplicatesCount)]
  #używam funkcji unique żeby usunąć powtarzające się rzędy i zmieniam data.table na data.frame
  wynik <- unique(wynik)
  wynik <- as.data.frame(wynik)
}

# Sprawdzenie rownowaznosci wynikow
compare(sql_5(Posts, PostLinks), base_5(Posts, PostLinks), allowAll = TRUE)
compare(sql_5(Posts, PostLinks), dplyr_5(Posts, PostLinks), allowAll = TRUE)
compare(sql_5(Posts, PostLinks), table_5(Posts, PostLinks), allowAll = TRUE)

# Porowanie czasow wykonania
microbenchmark(
  sqldf = sql_5(Posts, PostLinks),
  base = base_5(Posts, PostLinks),
  dplyr = dplyr_5(Posts, PostLinks),
  data.table = table_5(Posts, PostLinks),
  times = 5L
)
