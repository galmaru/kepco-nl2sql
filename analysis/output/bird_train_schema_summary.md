# BIRD Schema Summary

- Source: `BIRD train`
- Databases: 69
- Tables: 522
- Columns: 3539
- Questions: 9428
- Table samples: 3 rows per table

## Database Overview

| DB | 내용 | Tables | Columns | Rows | Questions |
|---|---|---:|---:|---:|---:|
| `european_football_1` | 주요 테이블: divisions, matchs | 2 | 11 | 123,425 | 57 |
| `sales_in_weather` | 주요 테이블: sales_in_weather, weather, relation | 3 | 26 | 4,638,162 | 80 |
| `craftbeer` | 주요 테이블: breweries, beers | 2 | 11 | 2,968 | 6 |
| `soccer_2016` | 주요 테이블: Batting_Style, Bowling_Style, City, Country, Extra_Type | 21 | 85 | 297,816 | 258 |
| `restaurant` | 주요 테이블: geographic, generalinfo, location | 3 | 12 | 19,297 | 117 |
| `movie` | 주요 테이블: actor, movie, characters | 3 | 27 | 7,659 | 46 |
| `olympics` | 주요 테이블: city, games, games_city, medal, noc_region | 11 | 32 | 701,801 | 169 |
| `language_corpus` | 주요 테이블: langs, pages, words, langs_words, pages_words | 6 | 24 | 157,378,539 | 136 |
| `app_store` | 주요 테이블: playstore, user_reviews | 2 | 15 | 75,126 | 63 |
| `sales` | 주요 테이블: Customers, Employees, Products, Sales | 4 | 16 | 6,735,506 | 107 |
| `video_games` | 주요 테이블: genre, game, platform, publisher, game_publisher | 8 | 21 | 105,319 | 201 |
| `image_and_language` | 주요 테이블: ATT_CLASSES, OBJ_CLASSES, IMG_OBJ, IMG_OBJ_ATT, PRED_CLASSES | 6 | 20 | 3,589,599 | 139 |
| `software_company` | 주요 테이블: Demog, mailings3, Customers, Mailings1_2, Sales | 5 | 38 | 4,141,029 | 75 |
| `authors` | 주요 테이블: Author, Conference, Journal, Paper, PaperAuthor | 5 | 21 | 4,837,220 | 174 |
| `movies_4` | 주요 테이블: country, department, gender, genre, keyword | 17 | 53 | 393,358 | 158 |
| `social_media` | 주요 테이블: location, user, twitter | 3 | 21 | 205,372 | 78 |
| `human_resources` | 주요 테이블: location, position, employee | 3 | 20 | 37 | 59 |
| `regional_sales` | 주요 테이블: Customers, Products, Regions, Sales Team, Store Locations | 6 | 41 | 8,531 | 164 |
| `computer_student` | 주요 테이블: course, person, advisedBy, taughtBy | 4 | 12 | 712 | 72 |
| `works_cycles` | 주요 테이블: CountryRegion, Culture, Currency, CountryRegionCurrency, Person | 65 | 455 | 719,435 | 474 |
| `food_inspection_2` | 주요 테이블: employee, establishment, inspection, inspection_point, violation | 5 | 40 | 701,342 | 139 |
| `citeseer` | 주요 테이블: cites, paper, content | 3 | 6 | 113,209 | 19 |
| `bike_share_1` | 주요 테이블: station, status, trip, weather | 4 | 46 | 72,647,070 | 113 |
| `law_episode` | 주요 테이블: Episode, Keyword, Person, Award, Credit | 6 | 41 | 3,350 | 114 |
| `cs_semester` | 주요 테이블: course, prof, RA, registration, student | 5 | 28 | 197 | 113 |
| `legislator` | 주요 테이블: current, current-terms, historical, historical-terms, social-media | 5 | 107 | 27,826 | 177 |
| `world` | 주요 테이블: City, Country, CountryLanguage | 3 | 24 | 5,302 | 99 |
| `cookbook` | 주요 테이블: Ingredient, Recipe, Nutrition, Quantity | 4 | 39 | 10,371 | 69 |
| `university` | 주요 테이블: country, ranking_system, ranking_criteria, university, university_ranking_year | 6 | 20 | 32,042 | 150 |
| `books` | 주요 테이블: address_status, author, book_language, country, address | 15 | 50 | 84,337 | 198 |
| `shipping` | 주요 테이블: city, customer, driver, truck, shipment | 5 | 32 | 1,684 | 106 |
| `food_inspection` | 주요 테이블: businesses, inspections, violations | 3 | 25 | 66,172 | 83 |
| `movie_platform` | 주요 테이블: lists, movies, ratings_users, lists_users, ratings | 5 | 55 | 20,200,856 | 167 |
| `shakespeare` | 주요 테이블: chapters, characters, paragraphs, works | 4 | 19 | 37,380 | 110 |
| `book_publishing_company` | 주요 테이블: authors, jobs, publishers, employee, pub_info | 11 | 64 | 254 | 73 |
| `car_retails` | 주요 테이블: offices, employees, customers, orders, payments | 8 | 59 | 3,864 | 126 |
| `mental_health_survey` | 주요 테이블: Question, Survey, Answer | 3 | 8 | 234,750 | 50 |
| `hockey` | 주요 테이블: AwardsMisc, HOF, Teams, Coaches, AwardsCoaches | 22 | 300 | 96,061 | 207 |
| `music_platform_2` | 주요 테이블: runs, podcasts, reviews, categories | 4 | 16 | 2,283,775 | 69 |
| `address` | 주요 테이블: CBSA, state, congress, zip_data, alias | 9 | 81 | 258,473 | 150 |
| `menu` | 주요 테이블: Dish, Menu, MenuPage, MenuItem | 4 | 45 | 1,845,587 | 108 |
| `professional_basketball` | 주요 테이블: awards_players, coaches, draft, player_allstar, players | 9 | 157 | 44,822 | 157 |
| `cars` | 주요 테이블: country, price, data, production | 4 | 16 | 1,491 | 84 |
| `synthea` | 주요 테이블: all_prevalences, patients, encounters, allergies, careplans | 11 | 85 | 170,810 | 185 |
| `genes` | 주요 테이블: Classification, Genes, Interactions | 3 | 15 | 6,118 | 23 |
| `retails` | 주요 테이블: customer, lineitem, nation, orders, part | 8 | 61 | 7,083,689 | 245 |
| `talkingdata` | 주요 테이블: app_all, app_events, app_events_relevant, app_labels, events | 12 | 48 | 40,645,703 | 206 |
| `beer_factory` | 주요 테이블: customers, geolocation, location, rootbeerbrand, rootbeer | 7 | 61 | 14,039 | 131 |
| `chicago_crime` | 주요 테이블: Community_Area, District, FBI_Code, IUCR, Neighborhood | 7 | 52 | 268,824 | 188 |
| `mondial_geo` | 주요 테이블: borders, city, continent, country, desert | 34 | 139 | 21,056 | 293 |
| `student_loan` | 주요 테이블: bool, person, disabled, enlist, filed_for_bankrupcy | 10 | 15 | 5,288 | 203 |
| `codebase_comments` | 주요 테이블: Method, MethodParameter, Repo, Solution | 4 | 25 | 9,119,319 | 124 |
| `retail_world` | 주요 테이블: Categories, Customers, Employees, Shippers, Suppliers | 8 | 42 | 932 | 373 |
| `music_tracker` | 주요 테이블: torrents, tags | 2 | 10 | 237,002 | 45 |
| `disney` | 주요 테이블: characters, director, movies_total_gross, revenue, voice-actors | 5 | 23 | 1,639 | 115 |
| `college_completion` | 주요 테이블: institution_details, institution_grads, state_sector_grads, state_sector_details | 4 | 102 | 1,391,154 | 76 |
| `ice_hockey_draft` | 주요 테이블: height_info, weight_info, PlayerInfo, SeasonStatus | 4 | 37 | 7,718 | 84 |
| `world_development_indicators` | 주요 테이블: Country, Series, CountryNotes, Footnotes, Indicators | 6 | 67 | 6,195,691 | 157 |
| `airline` | 주요 테이블: Air Carriers, Airports, Airlines | 3 | 32 | 709,518 | 92 |
| `retail_complains` | 주요 테이블: state, callcenterlogs, client, district, events | 6 | 58 | 33,289 | 168 |
| `trains` | 주요 테이블: cars, trains | 2 | 12 | 83 | 40 |
| `public_review_platform` | 주요 테이블: Attributes, Categories, Compliments, Days, Years | 15 | 75 | 990,846 | 381 |
| `donor` | 주요 테이블: essays, projects, donations, resources | 4 | 71 | 7,528,409 | 160 |
| `coinmarketcap` | 주요 테이블: coins, historical | 2 | 34 | 4,450,899 | 48 |
| `simpson_episodes` | 주요 테이블: Episode, Person, Award, Character_Award, Credit | 7 | 42 | 5,551 | 210 |
| `movie_3` | 주요 테이블: film_text, actor, address, category, city | 16 | 89 | 47,273 | 325 |
| `shooting` | 주요 테이블: incidents, officers, subjects | 3 | 20 | 812 | 28 |
| `superstore` | 주요 테이블: people, product, central_superstore, east_superstore, south_superstore | 6 | 61 | 27,787 | 116 |
| `movielens` | 주요 테이블: users, directors, actors, movies, movies2actors | 7 | 24 | 1,249,411 | 98 |

## european_football_1

주요 테이블: divisions, matchs

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `divisions` | divisions | 21 | 3 | `division`, `name`, `country` | division=B1; name=Division 1A; country=Belgium |
| `matchs` | matchs | 123,404 | 8 | `Div`, `Date`, `HomeTeam`, `AwayTeam`, `FTHG`, `FTAG`, `FTR` | Div=B1; Date=2020-08-08; HomeTeam=Club Brugge; AwayTeam=Charleroi |

## sales_in_weather

주요 테이블: sales_in_weather, weather, relation

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `sales_in_weather` | sales_in_weather | 4,617,600 | 4 | `date`, `store_nbr`, `item_nbr`, `units` | date=2012-01-01; store_nbr=1; item_nbr=1; units=0 |
| `weather` | weather | 20,517 | 20 | `station_nbr`, `date`, `tmax`, `tmin`, `tavg`, `depart`, `dewpoint` | station_nbr=1; date=2012-01-01; tmax=52; tmin=31 |
| `relation` | relation | 45 | 2 | `store_nbr`, `station_nbr` | store_nbr=1; station_nbr=1 |

## craftbeer

주요 테이블: breweries, beers

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `breweries` | Breweries | 558 | 4 | `id`, `name`, `city`, `state` | id=0; name=NorthGate Brewing ; city=Minneapolis; state=MN |
| `beers` | Beers | 2,410 | 7 | `id`, `brewery_id`, `abv`, `ibu`, `name`, `style`, `ounces` | id=1; brewery_id=166; abv=0.065; ibu=65.0 |

## soccer_2016

주요 테이블: Batting_Style, Bowling_Style, City, Country, Extra_Type

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Batting_Style` | Batting_Style | 2 | 2 | `Batting_Id`, `Batting_hand` | Batting_Id=1; Batting_hand=Left-hand bat |
| `Bowling_Style` | Bowling_Style | 14 | 2 | `Bowling_Id`, `Bowling_skill` | Bowling_Id=1; Bowling_skill=Right-arm medium |
| `City` | City | 29 | 3 | `City_Id`, `City_Name`, `Country_id` | City_Id=1; City_Name=Bangalore; Country_id=1 |
| `Country` | 국가 | 12 | 2 | `Country_Id`, `Country_Name` | Country_Id=1; Country_Name=India |
| `Extra_Type` | Extra_Type | 5 | 2 | `Extra_Id`, `Extra_Name` | Extra_Id=1; Extra_Name=legbyes |
| `Extra_Runs` | Extra_Runs | 7,469 | 6 | `Match_Id`, `Over_Id`, `Ball_Id`, `Innings_No`, `Extra_Type_Id`, `Extra_Runs` | Match_Id=335987; Over_Id=1; Ball_Id=1; Innings_No=1 |
| `Out_Type` | Out_Type | 9 | 2 | `Out_Id`, `Out_Name` | Out_Id=1; Out_Name=caught |
| `Outcome` | Outcome | 3 | 2 | `Outcome_Id`, `Outcome_Type` | Outcome_Id=1; Outcome_Type=Result |
| `Player` | 선수 기본 정보 | 469 | 6 | `Player_Id`, `Batting_hand`, `Bowling_skill`, `Country_Name`, `Player_Name`, `DOB` | Player_Id=1; Batting_hand=1; Bowling_skill=1; Country_Name=1 |
| `Rolee` | Rolee | 4 | 2 | `Role_Id`, `Role_Desc` | Role_Id=1; Role_Desc=Captain |
| `Season` | Season | 9 | 5 | `Season_Id`, `Man_of_the_Series`, `Orange_Cap`, `Purple_Cap`, `Season_Year` | Season_Id=1; Man_of_the_Series=32; Orange_Cap=100; Purple_Cap=102 |
| `Team` | 팀 기본 정보 | 13 | 2 | `Team_Id`, `Team_Name` | Team_Id=1; Team_Name=Kolkata Knight Riders |
| `Toss_Decision` | Toss_Decision | 2 | 2 | `Toss_Id`, `Toss_Name` | Toss_Id=1; Toss_Name=field |
| `Umpire` | Umpire | 52 | 3 | `Umpire_Id`, `Umpire_Country`, `Umpire_Name` | Umpire_Id=1; Umpire_Country=6; Umpire_Name=Asad Rauf |
| `Venue` | Venue | 35 | 3 | `Venue_Id`, `City_Id`, `Venue_Name` | Venue_Id=1; City_Id=1; Venue_Name=M Chinnaswamy Stadium |
| `Win_By` | Win_By | 4 | 2 | `Win_Id`, `Win_Type` | Win_Id=1; Win_Type=runs |
| `Match` | 축구 경기 상세 통계 | 577 | 13 | `Match_Id`, `Team_1`, `Team_2`, `Season_Id`, `Venue_Id`, `Toss_Winner`, `Toss_Decide` | Match_Id=335987; Team_1=2; Team_2=1; Season_Id=1 |
| `Ball_by_Ball` | Ball_by_Ball | 136,590 | 10 | `Match_Id`, `Over_Id`, `Ball_Id`, `Innings_No`, `Team_Batting`, `Team_Bowling`, `Striker_Batting_Position` | Match_Id=335987; Over_Id=1; Ball_Id=1; Innings_No=1 |
| `Batsman_Scored` | Batsman_Scored | 133,097 | 5 | `Match_Id`, `Over_Id`, `Ball_Id`, `Innings_No`, `Runs_Scored` | Match_Id=335987; Over_Id=1; Ball_Id=1; Innings_No=1 |
| `Player_Match` | Player_Match | 12,694 | 4 | `Match_Id`, `Player_Id`, `Role_Id`, `Team_Id` | Match_Id=335987; Player_Id=1; Role_Id=1; Team_Id=1 |
| `Wicket_Taken` | Wicket_Taken | 6,727 | 7 | `Match_Id`, `Over_Id`, `Ball_Id`, `Innings_No`, `Player_Out`, `Kind_Out`, `Fielders` | Match_Id=335987; Over_Id=2; Ball_Id=1; Innings_No=2 |

## restaurant

주요 테이블: geographic, generalinfo, location

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `geographic` | geographic | 168 | 3 | `city`, `county`, `region` | city=alameda; county=alameda county; region=bay area |
| `generalinfo` | generalinfo | 9,590 | 5 | `id_restaurant`, `city`, `label`, `food_type`, `review` | id_restaurant=1; city=san francisco; label=sparky's diner; food_type=24 hour diner |
| `location` | location | 9,539 | 4 | `id_restaurant`, `city`, `street_num`, `street_name` | id_restaurant=1; city=san francisco; street_num=242; street_name=church st |

## movie

주요 테이블: actor, movie, characters

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `actor` | actor | 2,713 | 10 | `ActorID`, `Name`, `Date of Birth`, `Birth City`, `Birth Country`, `Height (Inches)`, `Biography` | ActorID=1; Name=John Travolta; Date of Birth=1954-02-18; Birth City=Englewood |
| `movie` | movie | 634 | 11 | `MovieID`, `Title`, `MPAA Rating`, `Budget`, `Gross`, `Release Date`, `Genre` | MovieID=1; Title=Look Who's Talking; MPAA Rating=PG-13; Budget=7500000 |
| `characters` | characters | 4,312 | 6 | `MovieID`, `ActorID`, `Character Name`, `creditOrder`, `pay`, `screentime` | MovieID=1; ActorID=1; Character Name=James; creditOrder=1 |

## olympics

주요 테이블: city, games, games_city, medal, noc_region

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `city` | city | 42 | 2 | `id`, `city_name` | id=1; city_name=Barcelona |
| `games` | games | 51 | 4 | `id`, `games_year`, `games_name`, `season` | id=1; games_year=1992; games_name=1992 Summer; season=Summer |
| `games_city` | games_city | 52 | 2 | `games_id`, `city_id` | games_id=1; city_id=1 |
| `medal` | medal | 4 | 2 | `id`, `medal_name` | id=1; medal_name=Gold |
| `noc_region` | noc_region | 231 | 3 | `id`, `noc`, `region_name` | id=1; noc=AFG; region_name=Afghanistan |
| `person` | person | 128,854 | 5 | `id`, `full_name`, `gender`, `height`, `weight` | id=1; full_name=A Dijiang; gender=M; height=180 |
| `games_competitor` | games_competitor | 180,252 | 4 | `id`, `games_id`, `person_id`, `age` | id=1; games_id=1; person_id=1; age=24 |
| `person_region` | person_region | 130,521 | 2 | `person_id`, `region_id` | person_id=1; region_id=42 |
| `sport` | sport | 66 | 2 | `id`, `sport_name` | id=1; sport_name=Aeronautics |
| `event` | 동아리 행사 | 757 | 3 | `id`, `sport_id`, `event_name` | id=1; sport_id=9; event_name=Basketball Men's Basketball |
| `competitor_event` | competitor_event | 260,971 | 3 | `event_id`, `competitor_id`, `medal_id` | event_id=1; competitor_id=1; medal_id=4 |

## language_corpus

주요 테이블: langs, pages, words, langs_words, pages_words

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `langs` | langs | 1 | 5 | `lid`, `lang`, `locale`, `pages`, `words` | lid=1; lang=ca; locale=ca_ES; pages=1129144 |
| `pages` | pages | 1,129,144 | 6 | `pid`, `lid`, `page`, `revision`, `title`, `words` | pid=1; lid=1; page=1; revision=28236978 |
| `words` | words | 2,764,996 | 3 | `wid`, `word`, `occurrences` | wid=1; word=àbac; occurrences=242 |
| `langs_words` | langs_words | 2,764,996 | 3 | `lid`, `wid`, `occurrences` | lid=1; wid=1; occurrences=242 |
| `pages_words` | pages_words | 129,131,916 | 3 | `pid`, `wid`, `occurrences` | pid=1; wid=1; occurrences=30 |
| `biwords` | biwords | 21,587,486 | 4 | `lid`, `w1st`, `w2nd`, `occurrences` | lid=1; w1st=1; w2nd=2; occurrences=4 |

## app_store

주요 테이블: playstore, user_reviews

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `playstore` | googleplaystore | 10,840 | 10 | `App`, `Category`, `Rating`, `Reviews`, `Size`, `Installs`, `Type` | App=Photo Editor & Candy Camera & Grid & ScrapBook; Category=ART_AND_DESIGN; Rating=4.1; Reviews=159 |
| `user_reviews` | googleplaystore_user_reviews | 64,286 | 5 | `App`, `Translated_Review`, `Sentiment`, `Sentiment_Polarity`, `Sentiment_Subjectivity` | App=10 Best Foods for You; Translated_Review=I like eat delicious food. That's I'm cooking...; Sentiment=Positive; Sentiment_Polarity=1.0 |

## sales

주요 테이블: Customers, Employees, Products, Sales

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Customers` | Customers | 19,759 | 4 | `CustomerID`, `FirstName`, `MiddleInitial`, `LastName` | CustomerID=1; FirstName=Aaron; MiddleInitial=NULL; LastName=Alexander |
| `Employees` | Employees | 22 | 4 | `EmployeeID`, `FirstName`, `MiddleInitial`, `LastName` | EmployeeID=1; FirstName=Abraham; MiddleInitial=e; LastName=Bennet |
| `Products` | Products | 504 | 3 | `ProductID`, `Name`, `Price` | ProductID=1; Name=Adjustable Race; Price=1.6 |
| `Sales` | Sales | 6,715,221 | 5 | `SalesID`, `SalesPersonID`, `CustomerID`, `ProductID`, `Quantity` | SalesID=1; SalesPersonID=17; CustomerID=10482; ProductID=500 |

## video_games

주요 테이블: genre, game, platform, publisher, game_publisher

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `genre` | genre | 12 | 2 | `id`, `genre_name` | id=1; genre_name=Action |
| `game` | game | 11,317 | 3 | `id`, `genre_id`, `game_name` | id=44; genre_id=4; game_name=2 Games in 1: Sonic Advance & ChuChu Rocket! |
| `platform` | platform | 31 | 2 | `id`, `platform_name` | id=1; platform_name=Wii |
| `publisher` | 출판사 | 577 | 2 | `id`, `publisher_name` | id=1; publisher_name=10TACLE Studios |
| `game_publisher` | game_publisher | 11,732 | 3 | `id`, `game_id`, `publisher_id` | id=1; game_id=10866; publisher_id=369 |
| `game_platform` | game_platform | 16,326 | 4 | `id`, `game_publisher_id`, `platform_id`, `release_year` | id=1; game_publisher_id=8564; platform_id=4; release_year=2007 |
| `region` | region | 4 | 2 | `id`, `region_name` | id=1; region_name=North America |
| `region_sales` | region_sales | 65,320 | 3 | `region_id`, `game_platform_id`, `num_sales` | region_id=1; game_platform_id=50; num_sales=3.5 |

## image_and_language

주요 테이블: ATT_CLASSES, OBJ_CLASSES, IMG_OBJ, IMG_OBJ_ATT, PRED_CLASSES

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `ATT_CLASSES` | ATT_CLASSES | 699 | 2 | `ATT_CLASS_ID`, `ATT_CLASS` | ATT_CLASS_ID=0; ATT_CLASS=building s |
| `OBJ_CLASSES` | OBJ_CLASSES | 300 | 2 | `OBJ_CLASS_ID`, `OBJ_CLASS` | OBJ_CLASS_ID=0; OBJ_CLASS=awning |
| `IMG_OBJ` | IMG_OBJ | 1,750,617 | 7 | `IMG_ID`, `OBJ_SAMPLE_ID`, `OBJ_CLASS_ID`, `X`, `Y`, `W`, `H` | IMG_ID=1; OBJ_SAMPLE_ID=1; OBJ_CLASS_ID=298; X=0 |
| `IMG_OBJ_ATT` | IMG_OBJ_ATT | 1,074,674 | 3 | `IMG_ID`, `ATT_CLASS_ID`, `OBJ_SAMPLE_ID` | IMG_ID=1113; ATT_CLASS_ID=0; OBJ_SAMPLE_ID=21 |
| `PRED_CLASSES` | PRED_CLASSES | 150 | 2 | `PRED_CLASS_ID`, `PRED_CLASS` | PRED_CLASS_ID=0; PRED_CLASS=playing on |
| `IMG_REL` | IMG_REL | 763,159 | 4 | `IMG_ID`, `PRED_CLASS_ID`, `OBJ1_SAMPLE_ID`, `OBJ2_SAMPLE_ID` | IMG_ID=675; PRED_CLASS_ID=0; OBJ1_SAMPLE_ID=13; OBJ2_SAMPLE_ID=1 |

## software_company

주요 테이블: Demog, mailings3, Customers, Mailings1_2, Sales

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Demog` | Demog | 200 | 21 | `GEOID`, `INHABITANTS_K`, `INCOME_K`, `A_VAR1`, `A_VAR2`, `A_VAR3`, `A_VAR4` | GEOID=0; INHABITANTS_K=30.046; INCOME_K=2631.47; A_VAR1=6.084 |
| `mailings3` | mailings3 | 300,000 | 3 | `REFID`, `REF_DATE`, `RESPONSE` | REFID=60000; REF_DATE=2007-07-01 12:00:00.0; RESPONSE=false |
| `Customers` | Customers | 360,000 | 7 | `ID`, `GEOID`, `SEX`, `MARITAL_STATUS`, `EDUCATIONNUM`, `OCCUPATION`, `age` | ID=0; GEOID=61; SEX=Male; MARITAL_STATUS=Never-married |
| `Mailings1_2` | Mailings1_2 | 60,000 | 3 | `REFID`, `REF_DATE`, `RESPONSE` | REFID=0; REF_DATE=2007-02-01 12:00:00.0; RESPONSE=false |
| `Sales` | Sales | 3,420,829 | 4 | `EVENTID`, `REFID`, `EVENT_DATE`, `AMOUNT` | EVENTID=0; REFID=0; EVENT_DATE=2006-12-21 12:00:00.0; AMOUNT=17.907 |

## authors

주요 테이블: Author, Conference, Journal, Paper, PaperAuthor

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Author` | Author | 247,030 | 3 | `Id`, `Name`, `Affiliation` | Id=9; Name=Ernest Jordan; Affiliation=NULL |
| `Conference` | Conference | 4,545 | 4 | `Id`, `ShortName`, `FullName`, `HomePage` | Id=1; ShortName=IADIS; FullName=International Association for Development of ...; HomePage= |
| `Journal` | Journal | 15,151 | 4 | `Id`, `ShortName`, `FullName`, `HomePage` | Id=1; ShortName=ICOM; FullName=Zeitschrift Für Interaktive Und Kooperative M...; HomePage=http://www.i-com-media.de |
| `Paper` | Paper | 2,254,920 | 6 | `Id`, `ConferenceId`, `JournalId`, `Title`, `Year`, `Keyword` | Id=1; ConferenceId=167; JournalId=0; Title=Stitching videos streamed by mobile phones in... |
| `PaperAuthor` | PaperAuthor | 2,315,574 | 4 | `PaperId`, `AuthorId`, `Name`, `Affiliation` | PaperId=4; AuthorId=1456512; Name=ADAM G. JONES; Affiliation=NULL |

## movies_4

주요 테이블: country, department, gender, genre, keyword

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `country` | country | 88 | 3 | `country_id`, `country_iso_code`, `country_name` | country_id=128; country_iso_code=AE; country_name=United Arab Emirates |
| `department` | department | 12 | 2 | `department_id`, `department_name` | department_id=1; department_name=Camera |
| `gender` | 성별 코드 | 3 | 2 | `gender_id`, `gender` | gender_id=0; gender=Unspecified |
| `genre` | genre | 20 | 2 | `genre_id`, `genre_name` | genre_id=12; genre_name=Adventure |
| `keyword` | keyword | 9,794 | 2 | `keyword_id`, `keyword_name` | keyword_id=30; keyword_name=individual |
| `language` | language | 88 | 3 | `language_id`, `language_code`, `language_name` | language_id=24574; language_code=en; language_name=English |
| `language_role` | language_role | 2 | 2 | `role_id`, `language_role` | role_id=1; language_role=Original |
| `movie` | movie | 4,627 | 13 | `movie_id`, `title`, `budget`, `homepage`, `overview`, `popularity`, `release_date` | movie_id=5; title=Four Rooms; budget=4000000; homepage= |
| `movie_genres` | movie_genres | 12,160 | 2 | `movie_id`, `genre_id` | movie_id=5; genre_id=35 |
| `movie_languages` | movie_languages | 11,740 | 3 | `movie_id`, `language_id`, `language_role_id` | movie_id=5; language_id=24574; language_role_id=2 |
| `person` | person | 104,838 | 2 | `person_id`, `person_name` | person_id=1; person_name=George Lucas |
| `movie_crew` | movie_crew | 129,581 | 4 | `movie_id`, `person_id`, `department_id`, `job` | movie_id=285; person_id=120; department_id=1; job=Director of Photography |
| `production_company` | production_company | 5,047 | 2 | `company_id`, `company_name` | company_id=1; company_name=Lucasfilm |
| `production_country` | production_country | 6,436 | 2 | `movie_id`, `country_id` | movie_id=5; country_id=214 |
| `movie_cast` | movie_cast | 59,083 | 5 | `movie_id`, `person_id`, `gender_id`, `character_name`, `cast_order` | movie_id=285; person_id=85; gender_id=2; character_name=Captain Jack Sparrow |
| `movie_keywords` | movie_keywords | 36,162 | 2 | `movie_id`, `keyword_id` | movie_id=5; keyword_id=612 |
| `movie_company` | movie_company | 13,677 | 2 | `movie_id`, `company_id` | movie_id=5; company_id=14 |

## social_media

주요 테이블: location, user, twitter

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `location` | location | 6,211 | 5 | `LocationID`, `Country`, `State`, `StateCode`, `City` | LocationID=1; Country=Albania; State=Elbasan; StateCode=AL |
| `user` | user | 99,260 | 2 | `UserID`, `Gender` | UserID=tw-1267804344; Gender=Unknown |
| `twitter` | twitter | 99,901 | 14 | `TweetID`, `LocationID`, `UserID`, `Weekday`, `Hour`, `Day`, `Lang` | TweetID=tw-682712873332805633; LocationID=3751; UserID=tw-40932430; Weekday=Thursday |

## human_resources

주요 테이블: location, position, employee

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `location` | location | 8 | 6 | `locationID`, `locationcity`, `address`, `state`, `zipcode`, `officephone` | locationID=1; locationcity=Atlanta; address=450 Peachtree Rd; state=GA |
| `position` | position | 4 | 5 | `positionID`, `positiontitle`, `educationrequired`, `minsalary`, `maxsalary` | positionID=1; positiontitle=Account Representative; educationrequired=4 year degree; minsalary=US$25,000.00 |
| `employee` | employee | 25 | 9 | `ssn`, `positionID`, `locationID`, `lastname`, `firstname`, `hiredate`, `salary` | ssn=000-01-0000; positionID=2; locationID=2; lastname=Milgrom |

## regional_sales

주요 테이블: Customers, Products, Regions, Sales Team, Store Locations

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Customers` | Customers | 50 | 2 | `CustomerID`, `Customer Names` | CustomerID=1; Customer Names=Avon Corp |
| `Products` | Products | 47 | 2 | `ProductID`, `Product Name` | ProductID=1; Product Name=Cookware |
| `Regions` | Regions | 48 | 3 | `StateCode`, `State`, `Region` | StateCode=AL; State=Alabama; Region=South |
| `Sales Team` | Sales Team | 28 | 3 | `SalesTeamID`, `Sales Team`, `Region` | SalesTeamID=1; Sales Team=Adam Hernandez; Region=Northeast |
| `Store Locations` | Store Locations | 367 | 15 | `StoreID`, `StateCode`, `City Name`, `County`, `State`, `Type`, `Latitude` | StoreID=1; StateCode=AL; City Name=Birmingham; County=Shelby County/Jefferson County |
| `Sales Orders` | Sales Orders | 7,991 | 16 | `OrderNumber`, `_SalesTeamID`, `_CustomerID`, `_StoreID`, `_ProductID`, `Sales Channel`, `WarehouseCode` | OrderNumber=SO - 000101; _SalesTeamID=6; _CustomerID=15; _StoreID=259 |

## computer_student

주요 테이블: course, person, advisedBy, taughtBy

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `course` | course | 132 | 2 | `course_id`, `courseLevel` | course_id=0; courseLevel=Level_500 |
| `person` | person | 278 | 6 | `p_id`, `professor`, `student`, `hasPosition`, `inPhase`, `yearsInProgram` | p_id=3; professor=0; student=1; hasPosition=0 |
| `advisedBy` | advisedBy | 113 | 2 | `p_id`, `p_id_dummy` | p_id=96; p_id_dummy=5 |
| `taughtBy` | taughtBy | 189 | 2 | `course_id`, `p_id` | course_id=0; p_id=40 |

## works_cycles

주요 테이블: CountryRegion, Culture, Currency, CountryRegionCurrency, Person

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `CountryRegion` | CountryRegion | 239 | 3 | `CountryRegionCode`, `Name`, `ModifiedDate` | CountryRegionCode=Cou; Name=Name; ModifiedDate=ModifiedDate |
| `Culture` | Culture | 8 | 3 | `CultureID`, `Name`, `ModifiedDate` | CultureID=; Name=Invariant Language (Invariant Country); ModifiedDate=2008-04-30 00:00:00.0 |
| `Currency` | Currency | 105 | 3 | `CurrencyCode`, `Name`, `ModifiedDate` | CurrencyCode=AED; Name=Emirati Dirham; ModifiedDate=2008-04-30 00:00:00.0 |
| `CountryRegionCurrency` | CountryRegionCurrency | 109 | 3 | `CountryRegionCode`, `CurrencyCode`, `ModifiedDate` | CountryRegionCode=AE; CurrencyCode=AED; ModifiedDate=2014-02-08 10:17:21.0 |
| `Person` | Person | 19,972 | 13 | `BusinessEntityID`, `PersonType`, `NameStyle`, `Title`, `FirstName`, `MiddleName`, `LastName` | BusinessEntityID=1; PersonType=EM; NameStyle=0; Title=NULL |
| `BusinessEntityContact` | BusinessEntityContact | 909 | 5 | `BusinessEntityID`, `PersonID`, `ContactTypeID`, `rowguid`, `ModifiedDate` | BusinessEntityID=292; PersonID=291; ContactTypeID=11; rowguid=7D4D2DBC-4A44-48F5-911D-A63ABAFD5120 |
| `EmailAddress` | EmailAddress | 19,972 | 5 | `BusinessEntityID`, `EmailAddressID`, `EmailAddress`, `rowguid`, `ModifiedDate` | BusinessEntityID=1; EmailAddressID=1; EmailAddress=ken0@adventure-works.com; rowguid=8A1901E4-671B-431A-871C-EADB2942E9EE |
| `Employee` | Employee | 290 | 16 | `BusinessEntityID`, `NationalIDNumber`, `LoginID`, `OrganizationNode`, `OrganizationLevel`, `JobTitle`, `BirthDate` | BusinessEntityID=1; NationalIDNumber=295847284; LoginID=adventure-works\ken0; OrganizationNode=NULL |
| `Password` | Password | 19,972 | 5 | `BusinessEntityID`, `PasswordHash`, `PasswordSalt`, `rowguid`, `ModifiedDate` | BusinessEntityID=1; PasswordHash=pbFwXWE99vobT6g+vPWFy93NtUU/orrIWafF01hccfM=; PasswordSalt=bE3XiWw=; rowguid=329EACBE-C883-4F48-B8B6-17AA4627EFFF |
| `PersonCreditCard` | PersonCreditCard | 19,118 | 3 | `BusinessEntityID`, `CreditCardID`, `ModifiedDate` | BusinessEntityID=293; CreditCardID=17038; ModifiedDate=2013-07-31 00:00:00.0 |
| `ProductCategory` | ProductCategory | 4 | 4 | `ProductCategoryID`, `Name`, `rowguid`, `ModifiedDate` | ProductCategoryID=1; Name=Bikes; rowguid=CFBDA25C-DF71-47A7-B81B-64EE161AA37C; ModifiedDate=2008-04-30 00:00:00.0 |
| `ProductDescription` | ProductDescription | 762 | 4 | `ProductDescriptionID`, `Description`, `rowguid`, `ModifiedDate` | ProductDescriptionID=3; Description=Chromoly steel.; rowguid=301EED3A-1A82-4855-99CB-2AFE8290D641; ModifiedDate=2013-04-30 00:00:00.0 |
| `ProductModel` | ProductModel | 128 | 6 | `ProductModelID`, `Name`, `CatalogDescription`, `Instructions`, `rowguid`, `ModifiedDate` | ProductModelID=1; Name=Classic Vest; CatalogDescription=NULL; Instructions=NULL |
| `ProductModelProductDescriptionCulture` | ProductModelProductDescriptionCulture | 762 | 4 | `ProductModelID`, `ProductDescriptionID`, `CultureID`, `ModifiedDate` | ProductModelID=1; ProductDescriptionID=1199; CultureID=en; ModifiedDate=2013-04-30 00:00:00.0 |
| `ProductPhoto` | ProductPhoto | 100 | 6 | `ProductPhotoID`, `ThumbNailPhoto`, `ThumbnailPhotoFileName`, `LargePhoto`, `LargePhotoFileName`, `ModifiedDate` | ProductPhotoID=69; ThumbNailPhoto=0x47494638396150003100F70000E3E3FCA6ACB3F5F6F...; ThumbnailPhotoFileName=racer02_black_f_small.gif; LargePhoto=0x474946383961F0009500F70000D3D3FEE2E3FE86878... |
| `ProductSubcategory` | ProductSubcategory | 37 | 5 | `ProductSubcategoryID`, `ProductCategoryID`, `Name`, `rowguid`, `ModifiedDate` | ProductSubcategoryID=1; ProductCategoryID=1; Name=Mountain Bikes; rowguid=2D364ADE-264A-433C-B092-4FCBF3804E01 |
| `SalesReason` | SalesReason | 10 | 4 | `SalesReasonID`, `Name`, `ReasonType`, `ModifiedDate` | SalesReasonID=1; Name=Price; ReasonType=Other; ModifiedDate=2008-04-30 00:00:00.0 |
| `SalesTerritory` | SalesTerritory | 10 | 10 | `TerritoryID`, `CountryRegionCode`, `Name`, `Group`, `SalesYTD`, `SalesLastYear`, `CostYTD` | TerritoryID=1; CountryRegionCode=US; Name=Northwest; Group=North America |
| `SalesPerson` | SalesPerson | 17 | 9 | `BusinessEntityID`, `TerritoryID`, `SalesQuota`, `Bonus`, `CommissionPct`, `SalesYTD`, `SalesLastYear` | BusinessEntityID=274; TerritoryID=NULL; SalesQuota=NULL; Bonus=0.0 |
| `SalesPersonQuotaHistory` | SalesPersonQuotaHistory | 163 | 5 | `BusinessEntityID`, `QuotaDate`, `SalesQuota`, `rowguid`, `ModifiedDate` | BusinessEntityID=274; QuotaDate=2011-05-31 00:00:00.0; SalesQuota=28000.0; rowguid=99109BBF-8693-4587-BC23-6036EC89E1BE |
| `SalesTerritoryHistory` | SalesTerritoryHistory | 17 | 6 | `BusinessEntityID`, `TerritoryID`, `StartDate`, `EndDate`, `rowguid`, `ModifiedDate` | BusinessEntityID=275; TerritoryID=2; StartDate=2011-05-31 00:00:00.0; EndDate=2012-11-29 00:00:00.0 |
| `ScrapReason` | ScrapReason | 16 | 3 | `ScrapReasonID`, `Name`, `ModifiedDate` | ScrapReasonID=1; Name=Brake assembly not as ordered; ModifiedDate=2008-04-30 00:00:00.0 |
| `Shift` | Shift | 3 | 5 | `ShiftID`, `Name`, `StartTime`, `EndTime`, `ModifiedDate` | ShiftID=1; Name=Day; StartTime=07:00:00; EndTime=15:00:00 |
| `ShipMethod` | ShipMethod | 5 | 6 | `ShipMethodID`, `Name`, `ShipBase`, `ShipRate`, `rowguid`, `ModifiedDate` | ShipMethodID=1; Name=XRQ - TRUCK GROUND; ShipBase=3.95; ShipRate=0.99 |
| `SpecialOffer` | SpecialOffer | 16 | 11 | `SpecialOfferID`, `Description`, `DiscountPct`, `Type`, `Category`, `StartDate`, `EndDate` | SpecialOfferID=1; Description=No Discount; DiscountPct=0.0; Type=No Discount |
| `BusinessEntityAddress` | BusinessEntityAddress | 19,614 | 5 | `BusinessEntityID`, `AddressID`, `AddressTypeID`, `rowguid`, `ModifiedDate` | BusinessEntityID=1; AddressID=249; AddressTypeID=2; rowguid=3A5D0A00-6739-4DFE-A8F7-844CD9DEE3DF |
| `SalesTaxRate` | SalesTaxRate | 29 | 7 | `SalesTaxRateID`, `StateProvinceID`, `TaxType`, `TaxRate`, `Name`, `rowguid`, `ModifiedDate` | SalesTaxRateID=1; StateProvinceID=1; TaxType=1; TaxRate=14.0 |
| `Store` | Store | 701 | 6 | `BusinessEntityID`, `SalesPersonID`, `Name`, `Demographics`, `rowguid`, `ModifiedDate` | BusinessEntityID=292; SalesPersonID=279; Name=Next-Door Bike Store; Demographics=<StoreSurvey xmlns="http://schemas.microsoft.... |
| `SalesOrderHeaderSalesReason` | SalesOrderHeaderSalesReason | 27,647 | 3 | `SalesOrderID`, `SalesReasonID`, `ModifiedDate` | SalesOrderID=43697; SalesReasonID=5; ModifiedDate=2011-05-31 00:00:00.0 |
| `TransactionHistoryArchive` | TransactionHistoryArchive | 89,253 | 9 | `TransactionID`, `ProductID`, `ReferenceOrderID`, `ReferenceOrderLineID`, `TransactionDate`, `TransactionType`, `Quantity` | TransactionID=1; ProductID=1; ReferenceOrderID=1; ReferenceOrderLineID=1 |
| `UnitMeasure` | UnitMeasure | 38 | 3 | `UnitMeasureCode`, `Name`, `ModifiedDate` | UnitMeasureCode=BOX; Name=Boxes; ModifiedDate=2008-04-30 00:00:00.0 |
| `ProductCostHistory` | ProductCostHistory | 395 | 5 | `ProductID`, `StartDate`, `EndDate`, `StandardCost`, `ModifiedDate` | ProductID=707; StartDate=2011-05-31 00:00:00.0; EndDate=2012-05-29 00:00:00.0; StandardCost=12.0278 |
| `ProductDocument` | ProductDocument | 32 | 3 | `ProductID`, `DocumentNode`, `ModifiedDate` | ProductID=317; DocumentNode=/2/1/; ModifiedDate=2013-12-29 13:51:58.0 |
| `ProductInventory` | ProductInventory | 1,069 | 7 | `ProductID`, `LocationID`, `Shelf`, `Bin`, `Quantity`, `rowguid`, `ModifiedDate` | ProductID=1; LocationID=1; Shelf=A; Bin=1 |
| `ProductProductPhoto` | ProductProductPhoto | 504 | 4 | `ProductID`, `ProductPhotoID`, `Primary`, `ModifiedDate` | ProductID=1; ProductPhotoID=1; Primary=1; ModifiedDate=2008-03-31 00:00:00.0 |
| `ProductReview` | ProductReview | 4 | 8 | `ProductReviewID`, `ProductID`, `ReviewerName`, `ReviewDate`, `EmailAddress`, `Rating`, `Comments` | ProductReviewID=1; ProductID=709; ReviewerName=John Smith; ReviewDate=2013-09-18 00:00:00.0 |
| `ShoppingCartItem` | ShoppingCartItem | 3 | 6 | `ShoppingCartItemID`, `ProductID`, `ShoppingCartID`, `Quantity`, `DateCreated`, `ModifiedDate` | ShoppingCartItemID=2; ProductID=862; ShoppingCartID=14951; Quantity=3 |
| `SpecialOfferProduct` | SpecialOfferProduct | 538 | 4 | `SpecialOfferID`, `ProductID`, `rowguid`, `ModifiedDate` | SpecialOfferID=1; ProductID=680; rowguid=BB30B868-D86C-4557-8DB2-4B2D0A83A0FB; ModifiedDate=2011-04-01 00:00:00.0 |
| `SalesOrderDetail` | SalesOrderDetail | 121,317 | 11 | `SalesOrderDetailID`, `SalesOrderID`, `ProductID`, `SpecialOfferID`, `CarrierTrackingNumber`, `OrderQty`, `UnitPrice` | SalesOrderDetailID=1; SalesOrderID=43659; ProductID=776; SpecialOfferID=1 |
| `TransactionHistory` | TransactionHistory | 113,443 | 9 | `TransactionID`, `ProductID`, `ReferenceOrderID`, `ReferenceOrderLineID`, `TransactionDate`, `TransactionType`, `Quantity` | TransactionID=100000; ProductID=784; ReferenceOrderID=41590; ReferenceOrderLineID=0 |
| `Vendor` | Vendor | 104 | 8 | `BusinessEntityID`, `AccountNumber`, `Name`, `CreditRating`, `PreferredVendorStatus`, `ActiveFlag`, `PurchasingWebServiceURL` | BusinessEntityID=1492; AccountNumber=AUSTRALI0001; Name=Australia Bike Retailer; CreditRating=1 |
| `ProductVendor` | ProductVendor | 460 | 11 | `ProductID`, `BusinessEntityID`, `UnitMeasureCode`, `AverageLeadTime`, `StandardPrice`, `LastReceiptCost`, `LastReceiptDate` | ProductID=1; BusinessEntityID=1580; UnitMeasureCode=CS; AverageLeadTime=17 |
| `PurchaseOrderHeader` | PurchaseOrderHeader | 4,012 | 13 | `PurchaseOrderID`, `EmployeeID`, `VendorID`, `ShipMethodID`, `RevisionNumber`, `Status`, `OrderDate` | PurchaseOrderID=1; EmployeeID=258; VendorID=1580; ShipMethodID=3 |
| `PurchaseOrderDetail` | PurchaseOrderDetail | 8,845 | 11 | `PurchaseOrderDetailID`, `PurchaseOrderID`, `ProductID`, `DueDate`, `OrderQty`, `UnitPrice`, `LineTotal` | PurchaseOrderDetailID=1; PurchaseOrderID=1; ProductID=1; DueDate=2011-04-30 00:00:00.0 |
| `WorkOrder` | WorkOrder | 72,591 | 10 | `WorkOrderID`, `ProductID`, `ScrapReasonID`, `OrderQty`, `StockedQty`, `ScrappedQty`, `StartDate` | WorkOrderID=1; ProductID=722; ScrapReasonID=NULL; OrderQty=8 |
| `WorkOrderRouting` | WorkOrderRouting | 67,131 | 12 | `WorkOrderID`, `ProductID`, `OperationSequence`, `LocationID`, `ScheduledStartDate`, `ScheduledEndDate`, `ActualStartDate` | WorkOrderID=13; ProductID=747; OperationSequence=1; LocationID=10 |
| `Customer` | Customer | 0 | 7 | `CustomerID`, `PersonID`, `StoreID`, `TerritoryID`, `AccountNumber`, `rowguid`, `ModifiedDate` | - |
| `ProductListPriceHistory` | ProductListPriceHistory | 395 | 5 | `ProductID`, `StartDate`, `EndDate`, `ListPrice`, `ModifiedDate` | ProductID=707; StartDate=2011-05-31 00:00:00.0; EndDate=2012-05-29 00:00:00.0; ListPrice=33.6442 |
| `Address` | Address | 19,614 | 9 | `AddressID`, `StateProvinceID`, `AddressLine1`, `AddressLine2`, `City`, `PostalCode`, `SpatialLocation` | AddressID=1; StateProvinceID=79; AddressLine1=1970 Napa Ct.; AddressLine2=NULL |
| `AddressType` | AddressType | 6 | 4 | `AddressTypeID`, `Name`, `rowguid`, `ModifiedDate` | AddressTypeID=1; Name=Billing; rowguid=B84F78B1-4EFE-4A0E-8CB7-70E9F112F886; ModifiedDate=2008-04-30 00:00:00.0 |
| `BillOfMaterials` | BillOfMaterials | 2,679 | 9 | `BillOfMaterialsID`, `ProductAssemblyID`, `ComponentID`, `UnitMeasureCode`, `StartDate`, `EndDate`, `BOMLevel` | BillOfMaterialsID=1; ProductAssemblyID=807; ComponentID=1; UnitMeasureCode=EA |
| `BusinessEntity` | BusinessEntity | 20,777 | 3 | `BusinessEntityID`, `rowguid`, `ModifiedDate` | BusinessEntityID=1; rowguid=0C7D8F81-D7B1-4CF0-9C0A-4CD8B6B50087; ModifiedDate=2017-12-13 13:20:24.0 |
| `ContactType` | ContactType | 20 | 3 | `ContactTypeID`, `Name`, `ModifiedDate` | ContactTypeID=1; Name=Accounting Manager; ModifiedDate=2008-04-30 00:00:00.0 |
| `CurrencyRate` | CurrencyRate | 13,532 | 7 | `CurrencyRateID`, `FromCurrencyCode`, `ToCurrencyCode`, `CurrencyRateDate`, `AverageRate`, `EndOfDayRate`, `ModifiedDate` | CurrencyRateID=1; FromCurrencyCode=USD; ToCurrencyCode=ARS; CurrencyRateDate=2011-05-31 00:00:00.0 |
| `Department` | Department | 16 | 4 | `DepartmentID`, `Name`, `GroupName`, `ModifiedDate` | DepartmentID=1; Name=Engineering; GroupName=Research and Development; ModifiedDate=2008-04-30 00:00:00.0 |
| `EmployeeDepartmentHistory` | EmployeeDepartmentHistory | 296 | 6 | `BusinessEntityID`, `DepartmentID`, `ShiftID`, `StartDate`, `EndDate`, `ModifiedDate` | BusinessEntityID=1; DepartmentID=16; ShiftID=1; StartDate=2009-01-14 |
| `EmployeePayHistory` | EmployeePayHistory | 316 | 5 | `BusinessEntityID`, `RateChangeDate`, `Rate`, `PayFrequency`, `ModifiedDate` | BusinessEntityID=1; RateChangeDate=2009-01-14 00:00:00.0; Rate=125.5; PayFrequency=2 |
| `JobCandidate` | JobCandidate | 12 | 4 | `JobCandidateID`, `BusinessEntityID`, `Resume`, `ModifiedDate` | JobCandidateID=2; BusinessEntityID=NULL; Resume=<ns:Resume xmlns:ns="http://schemas.microsoft...; ModifiedDate=2007-06-23 00:00:00.0 |
| `Location` | Location | 14 | 5 | `LocationID`, `Name`, `CostRate`, `Availability`, `ModifiedDate` | LocationID=1; Name=Tool Crib; CostRate=0.0; Availability=0.0 |
| `PhoneNumberType` | PhoneNumberType | 3 | 3 | `PhoneNumberTypeID`, `Name`, `ModifiedDate` | PhoneNumberTypeID=1; Name=Cell; ModifiedDate=2017-12-13 13:19:22.0 |
| `Product` | Product | 504 | 25 | `ProductID`, `SizeUnitMeasureCode`, `WeightUnitMeasureCode`, `ProductSubcategoryID`, `ProductModelID`, `Name`, `ProductNumber` | ProductID=1; SizeUnitMeasureCode=NULL; WeightUnitMeasureCode=NULL; ProductSubcategoryID=NULL |
| `Document` | Document | 13 | 14 | `DocumentNode`, `Owner`, `DocumentLevel`, `Title`, `FolderFlag`, `FileName`, `FileExtension` | DocumentNode=/; Owner=217; DocumentLevel=0; Title=Documents |
| `StateProvince` | StateProvince | 181 | 8 | `StateProvinceID`, `CountryRegionCode`, `TerritoryID`, `StateProvinceCode`, `IsOnlyStateProvinceFlag`, `Name`, `rowguid` | StateProvinceID=1; CountryRegionCode=CA; TerritoryID=6; StateProvinceCode=AB |
| `CreditCard` | CreditCard | 19,118 | 6 | `CreditCardID`, `CardType`, `CardNumber`, `ExpMonth`, `ExpYear`, `ModifiedDate` | CreditCardID=1; CardType=SuperiorCard; CardNumber=33332664695310; ExpMonth=11 |
| `SalesOrderHeader` | SalesOrderHeader | 31,465 | 26 | `SalesOrderID`, `CustomerID`, `SalesPersonID`, `TerritoryID`, `BillToAddressID`, `ShipToAddressID`, `ShipMethodID` | SalesOrderID=43659; CustomerID=29825; SalesPersonID=279; TerritoryID=5 |

## food_inspection_2

주요 테이블: employee, establishment, inspection, inspection_point, violation

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `employee` | employee | 75 | 11 | `employee_id`, `supervisor`, `first_name`, `last_name`, `address`, `city`, `state` | employee_id=103705; supervisor=177316; first_name=Anastasia; last_name=Hansen |
| `establishment` | establishment | 31,642 | 12 | `license_no`, `dba_name`, `aka_name`, `facility_type`, `risk_level`, `address`, `city` | license_no=1; dba_name=HARVEST CRUSADES MINISTRIES; aka_name=NULL; facility_type=Special Event |
| `inspection` | inspection | 143,870 | 7 | `inspection_id`, `employee_id`, `license_no`, `followup_to`, `inspection_date`, `inspection_type`, `results` | inspection_id=44247; employee_id=141319; license_no=1222441; followup_to=NULL |
| `inspection_point` | inspection_point | 46 | 6 | `point_id`, `Description`, `category`, `code`, `fine`, `point_level` | point_id=1; Description=Source sound condition, no spoilage, foods pr...; category=Food Protection; code=7-38-005 (B) (B-2) |
| `violation` | violation | 525,709 | 4 | `inspection_id`, `point_id`, `fine`, `inspector_comment` | inspection_id=44247; point_id=30; fine=100; inspector_comment=All food not stored in the original container... |

## citeseer

주요 테이블: cites, paper, content

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `cites` | cites | 4,732 | 2 | `cited_paper_id`, `citing_paper_id` | cited_paper_id=100157; citing_paper_id=100157 |
| `paper` | paper | 3,312 | 2 | `paper_id`, `class_label` | paper_id=100157; class_label=Agents |
| `content` | content | 105,165 | 2 | `paper_id`, `word_cited_id` | paper_id=100157; word_cited_id=word1163 |

## bike_share_1

주요 테이블: station, status, trip, weather

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `station` | station | 70 | 7 | `id`, `name`, `lat`, `long`, `dock_count`, `city`, `installation_date` | id=2; name=San Jose Diridon Caltrain Station; lat=37.329732; long=-121.90178200000001 |
| `status` | 결과 상태 코드 | 71,984,434 | 4 | `station_id`, `bikes_available`, `docks_available`, `time` | station_id=2; bikes_available=2; docks_available=25; time=2013/08/29 12:06:01 |
| `trip` | trip | 658,901 | 11 | `id`, `duration`, `start_date`, `start_station_name`, `start_station_id`, `end_date`, `end_station_name` | id=4069; duration=174; start_date=8/29/2013 9:08; start_station_name=2nd at South Park |
| `weather` | weather | 3,665 | 24 | `date`, `max_temperature_f`, `mean_temperature_f`, `min_temperature_f`, `max_dew_point_f`, `mean_dew_point_f`, `min_dew_point_f` | date=8/29/2013; max_temperature_f=74; mean_temperature_f=68; min_temperature_f=61 |

## law_episode

주요 테이블: Episode, Keyword, Person, Award, Credit

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Episode` | Episode | 24 | 11 | `episode_id`, `series`, `season`, `episode`, `number_in_series`, `title`, `summary` | episode_id=tt0629204; series=Law and Order; season=9; episode=1 |
| `Keyword` | Keyword | 33 | 2 | `episode_id`, `keyword` | episode_id=tt0629239; keyword=nun |
| `Person` | Person | 800 | 9 | `person_id`, `name`, `birthdate`, `birth_name`, `birth_place`, `birth_region`, `birth_country` | person_id=nm0000210; name=Julia Roberts; birthdate=1967-10-28; birth_name=Julia Fiona Roberts |
| `Award` | Award | 22 | 10 | `award_id`, `episode_id`, `person_id`, `organization`, `year`, `award_category`, `award` | award_id=258; episode_id=tt0629149; person_id=nm0937725; organization=International Monitor Awards |
| `Credit` | Credit | 2,231 | 5 | `episode_id`, `person_id`, `category`, `role`, `credited` | episode_id=tt0629204; person_id=nm0226352; category=Additional Crew; role=technical advisor |
| `Vote` | Vote | 240 | 4 | `episode_id`, `stars`, `votes`, `percent` | episode_id=tt0629204; stars=10; votes=36; percent=17.7 |

## cs_semester

주요 테이블: course, prof, RA, registration, student

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `course` | course | 13 | 4 | `course_id`, `name`, `credit`, `diff` | course_id=1; name=Machine Learning Theory; credit=3; diff=3 |
| `prof` | prof | 10 | 8 | `prof_id`, `gender`, `first_name`, `last_name`, `email`, `popularity`, `teachingability` | prof_id=1; gender=Male; first_name=Nathaniel; last_name=Pigford |
| `RA` | RA | 35 | 4 | `student_id`, `prof_id`, `capability`, `salary` | student_id=16; prof_id=11; capability=2; salary=med |
| `registration` | registration | 101 | 4 | `course_id`, `student_id`, `grade`, `sat` | course_id=1; student_id=7; grade=A; sat=5 |
| `student` | student | 38 | 8 | `student_id`, `f_name`, `l_name`, `phone_number`, `email`, `intelligence`, `gpa` | student_id=1; f_name=Kerry; l_name=Pryor; phone_number=(243) 6836472 |

## legislator

주요 테이블: current, current-terms, historical, historical-terms, social-media

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `current` | current | 541 | 24 | `bioguide_id`, `cspan_id`, `ballotpedia_id`, `birthday_bio`, `fec_id`, `first_name`, `gender_bio` | bioguide_id=B000944; cspan_id=5051.0; ballotpedia_id=Sherrod Brown; birthday_bio=1952-11-09 |
| `current-terms` | current-terms | 3,078 | 23 | `bioguide`, `end`, `address`, `caucus`, `chamber`, `class`, `contact_form` | bioguide=B000944; end=1995-01-03; address=NULL; caucus=NULL |
| `historical` | historical | 11,864 | 26 | `bioguide_id`, `ballotpedia_id`, `bioguide_previous_id`, `birthday_bio`, `cspan_id`, `fec_id`, `first_name` | bioguide_id=B000226; ballotpedia_id=NULL; bioguide_previous_id=NULL; birthday_bio=1745-04-02 |
| `historical-terms` | historical-terms | 11,864 | 23 | `bioguide`, `address`, `chamber`, `class`, `contact_form`, `district`, `end` | bioguide=B000226; address=NULL; chamber=NULL; class=2.0 |
| `social-media` | social-media | 479 | 11 | `bioguide`, `facebook`, `facebook_id`, `govtrack`, `instagram`, `instagram_id`, `thomas` | bioguide=R000600; facebook=congresswomanaumuaamata; facebook_id=1537155909907320.0; govtrack=412664.0 |

## world

주요 테이블: City, Country, CountryLanguage

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `City` | City | 4,079 | 5 | `ID`, `CountryCode`, `Name`, `District`, `Population` | ID=1; CountryCode=AFG; Name=Kabul; District=Kabol |
| `Country` | 국가 | 239 | 15 | `Code`, `Name`, `Continent`, `Region`, `SurfaceArea`, `IndepYear`, `Population` | Code=ABW; Name=Aruba; Continent=North America; Region=Caribbean |
| `CountryLanguage` | CountryLanguage | 984 | 4 | `CountryCode`, `Language`, `IsOfficial`, `Percentage` | CountryCode=ABW; Language=Dutch; IsOfficial=T; Percentage=5.3 |

## cookbook

주요 테이블: Ingredient, Recipe, Nutrition, Quantity

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Ingredient` | Ingredient | 3,346 | 4 | `ingredient_id`, `category`, `name`, `plural` | ingredient_id=1; category=dairy; name=1% lowfat cottage cheese; plural=NULL |
| `Recipe` | Recipe | 1,031 | 11 | `recipe_id`, `title`, `subtitle`, `servings`, `yield_unit`, `prep_min`, `cook_min` | recipe_id=214; title=Raspberry Chiffon Pie; subtitle=NULL; servings=10 |
| `Nutrition` | Nutrition | 878 | 16 | `recipe_id`, `protein`, `carbo`, `alcohol`, `total_fat`, `sat_fat`, `cholestrl` | recipe_id=214; protein=5.47; carbo=41.29; alcohol=0.0 |
| `Quantity` | Quantity | 5,116 | 8 | `quantity_id`, `recipe_id`, `ingredient_id`, `max_qty`, `min_qty`, `unit`, `preparation` | quantity_id=1; recipe_id=214; ingredient_id=1613; max_qty=2.0 |

## university

주요 테이블: country, ranking_system, ranking_criteria, university, university_ranking_year

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `country` | country | 74 | 2 | `id`, `country_name` | id=1; country_name=Argentina |
| `ranking_system` | ranking_system | 3 | 2 | `id`, `system_name` | id=1; system_name=Times Higher Education World University Ranking |
| `ranking_criteria` | ranking_criteria | 21 | 3 | `id`, `ranking_system_id`, `criteria_name` | id=1; ranking_system_id=1; criteria_name=Teaching |
| `university` | university | 1,247 | 3 | `id`, `country_id`, `university_name` | id=1; country_id=73; university_name=Harvard University |
| `university_ranking_year` | university_ranking_year | 29,612 | 4 | `university_id`, `ranking_criteria_id`, `year`, `score` | university_id=1; ranking_criteria_id=1; year=2011; score=100 |
| `university_year` | university_year | 1,085 | 6 | `university_id`, `year`, `num_students`, `student_staff_ratio`, `pct_international_students`, `pct_female_students` | university_id=1; year=2011; num_students=20152; student_staff_ratio=8.9 |

## books

주요 테이블: address_status, author, book_language, country, address

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `address_status` | address_status | 2 | 2 | `status_id`, `address_status` | status_id=1; address_status=Active |
| `author` | author | 9,235 | 2 | `author_id`, `author_name` | author_id=1; author_name=A. Bartlett Giamatti |
| `book_language` | book_language | 27 | 3 | `language_id`, `language_code`, `language_name` | language_id=1; language_code=eng; language_name=English |
| `country` | country | 232 | 2 | `country_id`, `country_name` | country_id=1; country_name=Afghanistan |
| `address` | address | 1,000 | 5 | `address_id`, `country_id`, `street_number`, `street_name`, `city` | address_id=1; country_id=95; street_number=57; street_name=Glacier Hill Avenue |
| `customer` | customer | 2,000 | 4 | `customer_id`, `first_name`, `last_name`, `email` | customer_id=1; first_name=Ursola; last_name=Purdy; email=upurdy0@cdbaby.com |
| `customer_address` | customer_address | 3,350 | 3 | `customer_id`, `address_id`, `status_id` | customer_id=1; address_id=606; status_id=1 |
| `order_status` | order_status | 6 | 2 | `status_id`, `status_value` | status_id=1; status_value=Order Received |
| `publisher` | 출판사 | 2,264 | 2 | `publisher_id`, `publisher_name` | publisher_id=1; publisher_name=10/18 |
| `book` | book | 11,127 | 7 | `book_id`, `language_id`, `publisher_id`, `title`, `isbn13`, `num_pages`, `publication_date` | book_id=1; language_id=2; publisher_id=1010; title=The World's First Love: Mary  Mother of God |
| `book_author` | book_author | 17,642 | 2 | `book_id`, `author_id` | book_id=10539; author_id=1 |
| `shipping_method` | shipping_method | 4 | 3 | `method_id`, `method_name`, `cost` | method_id=1; method_name=Standard; cost=5.9 |
| `cust_order` | cust_order | 7,550 | 5 | `order_id`, `customer_id`, `shipping_method_id`, `dest_address_id`, `order_date` | order_id=1; customer_id=387; shipping_method_id=4; dest_address_id=1 |
| `order_history` | order_history | 22,348 | 4 | `history_id`, `order_id`, `status_id`, `status_date` | history_id=1; order_id=1; status_id=1; status_date=2021-07-14 17:04:28 |
| `order_line` | order_line | 7,550 | 4 | `line_id`, `order_id`, `book_id`, `price` | line_id=1024; order_id=2051; book_id=10720; price=3.19 |

## shipping

주요 테이블: city, customer, driver, truck, shipment

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `city` | city | 601 | 5 | `city_id`, `city_name`, `state`, `population`, `area` | city_id=100; city_name=Union City; state=New Jersey; population=67088 |
| `customer` | customer | 100 | 9 | `cust_id`, `cust_name`, `annual_revenue`, `cust_type`, `address`, `city`, `state` | cust_id=193; cust_name=Advanced Fabricators; annual_revenue=39588651; cust_type=manufacturer |
| `driver` | driver | 11 | 8 | `driver_id`, `first_name`, `last_name`, `address`, `city`, `state`, `zip_code` | driver_id=20; first_name=Sue; last_name=Newell; address=268 Richmond Ave |
| `truck` | truck | 12 | 3 | `truck_id`, `make`, `model_year` | truck_id=1; make=Peterbilt; model_year=2005 |
| `shipment` | shipment | 960 | 7 | `ship_id`, `cust_id`, `truck_id`, `driver_id`, `city_id`, `weight`, `ship_date` | ship_id=1000; cust_id=3660; truck_id=1; driver_id=23 |

## food_inspection

주요 테이블: businesses, inspections, violations

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `businesses` | businesses | 6,358 | 16 | `business_id`, `name`, `address`, `city`, `postal_code`, `latitude`, `longitude` | business_id=10; name=Tiramisu Kitchen; address=033 Belden Pl; city=San Francisco |
| `inspections` | inspections | 23,764 | 4 | `business_id`, `score`, `date`, `type` | business_id=10; score=92; date=2014-01-14; type=Routine - Unscheduled |
| `violations` | violations | 36,050 | 5 | `business_id`, `date`, `violation_type_id`, `risk_category`, `description` | business_id=10; date=2014-07-29; violation_type_id=103129; risk_category=Moderate Risk |

## movie_platform

주요 테이블: lists, movies, ratings_users, lists_users, ratings

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `lists` | lists | 79,565 | 14 | `list_id`, `user_id`, `list_title`, `list_movie_number`, `list_update_timestamp_utc`, `list_creation_timestamp_utc`, `list_followers` | list_id=1; user_id=88260493; list_title=Films that made your kid sister cry; list_movie_number=5 |
| `movies` | movies | 226,087 | 10 | `movie_id`, `movie_title`, `movie_release_year`, `movie_url`, `movie_title_language`, `movie_popularity`, `movie_image_url` | movie_id=1; movie_title=La Antena; movie_release_year=2007; movie_url=http://mubi.com/films/la-antena |
| `ratings_users` | ratings_users | 4,297,641 | 8 | `user_id`, `rating_date_utc`, `user_trialist`, `user_subscriber`, `user_avatar_image_url`, `user_cover_image_url`, `user_eligible_for_trial` | user_id=41579158; rating_date_utc=2017-06-10; user_trialist=0; user_subscriber=0 |
| `lists_users` | lists_users | 80,311 | 10 | `user_id`, `list_id`, `list_update_date_utc`, `list_creation_date_utc`, `user_trialist`, `user_subscriber`, `user_avatar_image_url` | user_id=85981819; list_id=1969; list_update_date_utc=2019-11-26; list_creation_date_utc=2009-12-18 |
| `ratings` | ratings | 15,517,252 | 13 | `movie_id`, `rating_id`, `user_id`, `rating_url`, `rating_score`, `rating_timestamp_utc`, `critic` | movie_id=1066; rating_id=15610495; user_id=41579158; rating_url=http://mubi.com/films/pavee-lackeen-the-trave... |

## shakespeare

주요 테이블: chapters, characters, paragraphs, works

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `chapters` | chapters | 945 | 5 | `id`, `work_id`, `Act`, `Scene`, `Description` | id=18704; work_id=1; Act=1; Scene=1 |
| `characters` | characters | 1,266 | 4 | `id`, `CharName`, `Abbrev`, `Description` | id=1; CharName=First Apparition; Abbrev=First Apparition; Description= |
| `paragraphs` | paragraphs | 35,126 | 5 | `id`, `character_id`, `chapter_id`, `ParagraphNum`, `PlainText` | id=630863; character_id=1261; chapter_id=18704; ParagraphNum=3 |
| `works` | works | 43 | 5 | `id`, `Title`, `LongTitle`, `Date`, `GenreType` | id=1; Title=Twelfth Night; LongTitle=Twelfth Night, Or What You Will; Date=1599 |

## book_publishing_company

주요 테이블: authors, jobs, publishers, employee, pub_info

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `authors` | authors | 23 | 9 | `au_id`, `au_lname`, `au_fname`, `phone`, `address`, `city`, `state` | au_id=172-32-1176; au_lname=White; au_fname=Johnson; phone=408 496-7223 |
| `jobs` | jobs | 14 | 4 | `job_id`, `job_desc`, `min_lvl`, `max_lvl` | job_id=1; job_desc=New Hire - Job not specified; min_lvl=10; max_lvl=10 |
| `publishers` | publishers | 8 | 5 | `pub_id`, `pub_name`, `city`, `state`, `country` | pub_id=0736; pub_name=New Moon Books; city=Boston; state=MA |
| `employee` | employee | 43 | 8 | `emp_id`, `job_id`, `pub_id`, `fname`, `minit`, `lname`, `job_lvl` | emp_id=A-C71970F; job_id=10; pub_id=1389; fname=Aria |
| `pub_info` | pub_info | 7 | 3 | `pub_id`, `logo`, `pr_info` | pub_id=0877; logo=0x4749463839618B002F00B30F0000000080000000800...; pr_info=This is sample text data for Binnet & Hardley... |
| `stores` | stores | 6 | 6 | `stor_id`, `stor_name`, `stor_address`, `city`, `state`, `zip` | stor_id=6380; stor_name=Eric the Read Books; stor_address=788 Catamaugus Ave.; city=Seattle |
| `discounts` | discounts | 3 | 5 | `stor_id`, `discounttype`, `lowqty`, `highqty`, `discount` | stor_id=NULL; discounttype=Initial Customer; lowqty=NULL; highqty=NULL |
| `titles` | titles | 18 | 10 | `title_id`, `pub_id`, `title`, `type`, `price`, `advance`, `royalty` | title_id=BU1032; pub_id=1389; title=The Busy Executive's Database Guide; type=business |
| `roysched` | roysched | 86 | 4 | `title_id`, `lorange`, `hirange`, `royalty` | title_id=BU1032; lorange=0; hirange=5000; royalty=10 |
| `sales` | sales | 21 | 6 | `stor_id`, `ord_num`, `title_id`, `ord_date`, `qty`, `payterms` | stor_id=6380; ord_num=6871; title_id=BU1032; ord_date=1994-09-14 00:00:00.0 |
| `titleauthor` | titleauthor | 25 | 4 | `au_id`, `title_id`, `au_ord`, `royaltyper` | au_id=172-32-1176; title_id=PS3333; au_ord=1; royaltyper=100 |

## car_retails

주요 테이블: offices, employees, customers, orders, payments

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `offices` | offices | 7 | 9 | `officeCode`, `city`, `phone`, `addressLine1`, `addressLine2`, `state`, `country` | officeCode=1; city=San Francisco; phone=+1 650 219 4782; addressLine1=100 Market Street |
| `employees` | employees | 23 | 8 | `employeeNumber`, `officeCode`, `reportsTo`, `lastName`, `firstName`, `extension`, `email` | employeeNumber=1002; officeCode=1; reportsTo=NULL; lastName=Murphy |
| `customers` | 고객 ID, 세그먼트, 통화 | 122 | 13 | `customerNumber`, `salesRepEmployeeNumber`, `customerName`, `contactLastName`, `contactFirstName`, `phone`, `addressLine1` | customerNumber=103; salesRepEmployeeNumber=1370; customerName=Atelier graphique; contactLastName=Schmitt |
| `orders` | orders | 326 | 7 | `orderNumber`, `customerNumber`, `orderDate`, `requiredDate`, `shippedDate`, `status`, `comments` | orderNumber=10100; customerNumber=363; orderDate=2003-01-06; requiredDate=2003-01-13 |
| `payments` | payments | 273 | 4 | `customerNumber`, `checkNumber`, `paymentDate`, `amount` | customerNumber=103; checkNumber=HQ336336; paymentDate=2004-10-19; amount=6066.78 |
| `productlines` | productlines | 7 | 4 | `productLine`, `textDescription`, `htmlDescription`, `image` | productLine=Classic Cars; textDescription=Attention car enthusiasts: Make your wildest ...; htmlDescription=NULL; image=NULL |
| `products` | 상품 ID와 설명 | 110 | 9 | `productCode`, `productLine`, `productName`, `productScale`, `productVendor`, `productDescription`, `quantityInStock` | productCode=S10_1678; productLine=Motorcycles; productName=1969 Harley Davidson Ultimate Chopper; productScale=1:10 |
| `orderdetails` | orderdetails | 2,996 | 5 | `orderNumber`, `productCode`, `quantityOrdered`, `priceEach`, `orderLineNumber` | orderNumber=10100; productCode=S18_1749; quantityOrdered=30; priceEach=136.0 |

## mental_health_survey

주요 테이블: Question, Survey, Answer

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Question` | Question | 105 | 2 | `questionid`, `questiontext` | questionid=1; questiontext=What is your age? |
| `Survey` | Survey | 5 | 2 | `SurveyID`, `Description` | SurveyID=2014; Description=mental health survey for 2014 |
| `Answer` | Answer | 234,640 | 4 | `UserID`, `QuestionID`, `SurveyID`, `AnswerText` | UserID=1; QuestionID=1; SurveyID=2014; AnswerText=37 |

## hockey

주요 테이블: AwardsMisc, HOF, Teams, Coaches, AwardsCoaches

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `AwardsMisc` | AwardsMisc | 124 | 6 | `name`, `ID`, `award`, `year`, `lgID`, `note` | name=1960 U.S. Olympic Hockey Team; ID=NULL; award=Patrick; year=2001 |
| `HOF` | HOF | 365 | 4 | `hofID`, `year`, `name`, `category` | hofID=abelsi01h; year=1969; name=Sid Abel; category=Player |
| `Teams` | Teams | 1,519 | 27 | `year`, `tmID`, `lgID`, `franchID`, `confID`, `divID`, `rank` | year=1909; tmID=COB; lgID=NHA; franchID=BKN |
| `Coaches` | Coaches | 1,812 | 14 | `coachID`, `year`, `tmID`, `stint`, `lgID`, `notes`, `g` | coachID=abelsi01c; year=1952; tmID=CHI; stint=1 |
| `AwardsCoaches` | AwardsCoaches | 77 | 5 | `coachID`, `award`, `year`, `lgID`, `note` | coachID=patrile01c; award=First Team All-Star; year=1930; lgID=NHL |
| `Master` | Master | 7,761 | 31 | `coachID`, `playerID`, `hofID`, `firstName`, `lastName`, `nameNote`, `nameGiven` | coachID=NULL; playerID=aaltoan01; hofID=NULL; firstName=Antti |
| `AwardsPlayers` | AwardsPlayers | 2,091 | 6 | `playerID`, `award`, `year`, `lgID`, `note`, `pos` | playerID=abelsi01; award=First Team All-Star; year=1948; lgID=NHL |
| `CombinedShutouts` | CombinedShutouts | 54 | 8 | `IDgoalie1`, `IDgoalie2`, `year`, `month`, `date`, `tmID`, `oppID` | IDgoalie1=chabolo01; IDgoalie2=grantbe01; year=1929; month=3 |
| `Goalies` | Goalies | 4,278 | 23 | `playerID`, `year`, `stint`, `tmID`, `lgID`, `GP`, `Min` | playerID=abbotge01; year=1943; stint=1; tmID=BOS |
| `GoaliesSC` | GoaliesSC | 31 | 11 | `playerID`, `year`, `tmID`, `lgID`, `GP`, `Min`, `W` | playerID=benedcl01; year=1914; tmID=OT1; lgID=NHA |
| `GoaliesShootout` | GoaliesShootout | 480 | 8 | `playerID`, `year`, `tmID`, `stint`, `W`, `L`, `SA` | playerID=aebisda01; year=2005; tmID=COL; stint=1 |
| `Scoring` | Scoring | 45,967 | 31 | `playerID`, `year`, `tmID`, `stint`, `lgID`, `pos`, `GP` | playerID=aaltoan01; year=1997; tmID=ANA; stint=1 |
| `ScoringSC` | ScoringSC | 284 | 10 | `playerID`, `year`, `tmID`, `lgID`, `pos`, `GP`, `G` | playerID=adamsbi01; year=1920; tmID=VML; lgID=PCHA |
| `ScoringShootout` | ScoringShootout | 2,072 | 7 | `playerID`, `year`, `tmID`, `stint`, `S`, `G`, `GDG` | playerID=adamske01; year=2006; tmID=PHO; stint=1 |
| `ScoringSup` | ScoringSup | 137 | 4 | `playerID`, `year`, `PPA`, `SHA` | playerID=actonke01; year=1988; PPA=NULL; SHA=1 |
| `SeriesPost` | SeriesPost | 832 | 13 | `year`, `tmIDWinner`, `tmIDLoser`, `round`, `series`, `lgIDWinner`, `lgIDLoser` | year=1912; tmIDWinner=VA1; tmIDLoser=QU1; round=SCF |
| `TeamSplits` | TeamSplits | 1,519 | 43 | `year`, `tmID`, `lgID`, `hW`, `hL`, `hT`, `hOTL` | year=1909; tmID=COB; lgID=NHA; hW=2 |
| `TeamVsTeam` | TeamVsTeam | 25,602 | 8 | `year`, `tmID`, `oppID`, `lgID`, `W`, `L`, `T` | year=1909; tmID=COB; oppID=HAI; lgID=NHA |
| `TeamsHalf` | TeamsHalf | 41 | 11 | `year`, `tmID`, `half`, `lgID`, `rank`, `G`, `W` | year=1916; tmID=MOC; half=1; lgID=NHA |
| `TeamsPost` | TeamsPost | 927 | 17 | `year`, `tmID`, `lgID`, `G`, `W`, `L`, `T` | year=1913; tmID=MOC; lgID=NHA; G=2 |
| `TeamsSC` | TeamsSC | 30 | 10 | `year`, `tmID`, `lgID`, `G`, `W`, `L`, `T` | year=1912; tmID=QU1; lgID=NHA; G=3 |
| `abbrev` | abbrev | 58 | 3 | `Type`, `Code`, `Fullname` | Type=Conference; Code=CC; Fullname=Campbell Conference |

## music_platform_2

주요 테이블: runs, podcasts, reviews, categories

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `runs` | runs | 12 | 3 | `run_at`, `max_rowid`, `reviews_added` | run_at=2021-05-10 02:53:00; max_rowid=3266481; reviews_added=1215223 |
| `podcasts` | podcasts | 108,578 | 5 | `podcast_id`, `itunes_id`, `slug`, `itunes_url`, `title` | podcast_id=a00018b54eb342567c94dacfb2a3e504; itunes_id=1313466221; slug=scaling-global; itunes_url=https://podcasts.apple.com/us/podcast/scaling... |
| `reviews` | reviews | 1,964,856 | 6 | `podcast_id`, `title`, `content`, `rating`, `author_id`, `created_at` | podcast_id=c61aa81c9b929a66f0c1db6cbe5d8548; title=really interesting!; content=Thanks for providing these insights.  Really ...; rating=5 |
| `categories` | categories | 210,329 | 2 | `podcast_id`, `category` | podcast_id=c61aa81c9b929a66f0c1db6cbe5d8548; category=arts |

## address

주요 테이블: CBSA, state, congress, zip_data, alias

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `CBSA` | CBSA | 465 | 3 | `CBSA`, `CBSA_name`, `CBSA_type` | CBSA=10300; CBSA_name=Adrian, MI; CBSA_type=Micro |
| `state` | state | 62 | 2 | `abbreviation`, `name` | abbreviation=AA; name=Armed Forces Americas |
| `congress` | congress | 540 | 10 | `cognress_rep_id`, `abbreviation`, `first_name`, `last_name`, `CID`, `party`, `state` | cognress_rep_id=AK; abbreviation=AK; first_name=Young; last_name=Don |
| `zip_data` | zip_data | 41,563 | 55 | `zip_code`, `state`, `CBSA`, `city`, `multi_county`, `type`, `organization` | zip_code=501; state=NY; CBSA=35620; city=Holtsville |
| `alias` | alias | 41,701 | 2 | `zip_code`, `alias` | zip_code=501; alias=Holtsville |
| `area_code` | area_code | 53,796 | 2 | `zip_code`, `area_code` | zip_code=501; area_code=631 |
| `avoid` | avoid | 24,114 | 2 | `zip_code`, `bad_alias` | zip_code=501; bad_alias=Internal Revenue Service |
| `country` | country | 51,001 | 3 | `zip_code`, `county`, `state` | zip_code=501; county=SUFFOLK; state=NY |
| `zip_congress` | zip_congress | 45,231 | 2 | `zip_code`, `district` | zip_code=501; district=NY-1 |

## menu

주요 테이블: Dish, Menu, MenuPage, MenuItem

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Dish` | Dish | 426,713 | 9 | `id`, `name`, `description`, `menus_appeared`, `times_appeared`, `first_appeared`, `last_appeared` | id=1; name=Consomme printaniere royal; description=NULL; menus_appeared=8 |
| `Menu` | Menu | 17,527 | 20 | `id`, `name`, `sponsor`, `event`, `venue`, `place`, `physical_description` | id=12463; name=NULL; sponsor=HOTEL EASTMAN; event=BREAKFAST |
| `MenuPage` | MenuPage | 66,937 | 7 | `id`, `menu_id`, `page_number`, `image_id`, `full_height`, `full_width`, `uuid` | id=119; menu_id=12460; page_number=1; image_id=1603595.0 |
| `MenuItem` | MenuItem | 1,334,410 | 9 | `id`, `menu_page_id`, `dish_id`, `price`, `high_price`, `created_at`, `updated_at` | id=1; menu_page_id=1389; dish_id=1; price=0.4 |

## professional_basketball

주요 테이블: awards_players, coaches, draft, player_allstar, players

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `awards_players` | awards_players | 1,719 | 6 | `playerID`, `award`, `year`, `lgID`, `note`, `pos` | playerID=abdulka01; award=All-Defensive Second Team; year=1969; lgID=NBA |
| `coaches` | coaches | 1,689 | 9 | `coachID`, `year`, `tmID`, `stint`, `lgID`, `won`, `lost` | coachID=adelmri01; year=1988; tmID=POR; stint=2 |
| `draft` | draft | 8,621 | 12 | `id`, `draftYear`, `tmID`, `draftRound`, `draftSelection`, `draftOverall`, `firstName` | id=1; draftYear=1967; tmID=ANA; draftRound=0 |
| `player_allstar` | player_allstar | 1,608 | 23 | `playerID`, `season_id`, `last_name`, `first_name`, `conference`, `league_id`, `games_played` | playerID=abdulka01; season_id=1969; last_name=Abdul-Jabbar; first_name=Kareem |
| `players` | players | 5,062 | 26 | `playerID`, `useFirst`, `firstName`, `middleName`, `lastName`, `nameGiven`, `fullGivenName` | playerID=abdelal01; useFirst=Alaa; firstName=Alaa; middleName=NULL |
| `teams` | teams | 1,536 | 22 | `year`, `tmID`, `lgID`, `franchID`, `confID`, `divID`, `rank` | year=1937; tmID=AFS; lgID=NBL; franchID=AFS |
| `awards_coaches` | awards_coaches | 61 | 6 | `id`, `year`, `coachID`, `award`, `lgID`, `note` | id=1; year=1962; coachID=gallaha01; award=NBA Coach of the Year |
| `players_teams` | players_teams | 23,751 | 43 | `id`, `playerID`, `year`, `tmID`, `stint`, `lgID`, `GP` | id=1; playerID=abdelal01; year=1990; tmID=POR |
| `series_post` | series_post | 775 | 10 | `id`, `year`, `tmIDWinner`, `tmIDLoser`, `round`, `series`, `lgIDWinner` | id=1; year=1946; tmIDWinner=PHW; tmIDLoser=CHS |

## cars

주요 테이블: country, price, data, production

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `country` | country | 3 | 2 | `origin`, `country` | origin=1; country=USA |
| `price` | price | 398 | 2 | `ID`, `price` | ID=1; price=25561.59078 |
| `data` | data | 398 | 9 | `ID`, `mpg`, `cylinders`, `displacement`, `horsepower`, `weight`, `acceleration` | ID=1; mpg=18.0; cylinders=8; displacement=307.0 |
| `production` | production | 692 | 3 | `ID`, `model_year`, `country` | ID=1; model_year=1970; country=1 |

## synthea

주요 테이블: all_prevalences, patients, encounters, allergies, careplans

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `all_prevalences` | all_prevalences | 244 | 6 | `ITEM`, `POPULATION TYPE`, `OCCURRENCES`, `POPULATION COUNT`, `PREVALENCE RATE`, `PREVALENCE PERCENTAGE` | ITEM=Viral Sinusitis (Disorder); POPULATION TYPE=LIVING; OCCURRENCES=868; POPULATION COUNT=1000 |
| `patients` | patients | 1,462 | 17 | `patient`, `birthdate`, `deathdate`, `ssn`, `drivers`, `passport`, `prefix` | patient=4ee2c837-e60f-4c54-9fdf-8686bc70760b; birthdate=1929-04-08; deathdate=2029-11-11; ssn=999-78-5976 |
| `encounters` | encounters | 20,524 | 7 | `ID`, `PATIENT`, `DATE`, `CODE`, `DESCRIPTION`, `REASONCODE`, `REASONDESCRIPTION` | ID=5114a5b4-64b8-47b2-82a6-0ce24aae0943; PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; DATE=2008-03-11; CODE=185349003 |
| `allergies` | allergies | 572 | 6 | `PATIENT`, `ENCOUNTER`, `CODE`, `START`, `STOP`, `DESCRIPTION` | PATIENT=ab6d8296-d3c7-4fef-9215-40b156db67ac; ENCOUNTER=9d87c22d-a777-426b-b020-cfa469229f82; CODE=425525006; START=3/11/95 |
| `careplans` | careplans | 12,125 | 9 | `PATIENT`, `ENCOUNTER`, `ID`, `START`, `STOP`, `CODE`, `DESCRIPTION` | PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=4d451e22-a354-40c9-8b33-b6126158666d; ID=e031962d-d13d-4ede-a449-040417d5e4fb; START=2009-01-11 |
| `conditions` | conditions | 7,040 | 6 | `PATIENT`, `ENCOUNTER`, `DESCRIPTION`, `START`, `STOP`, `CODE` | PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=4d451e22-a354-40c9-8b33-b6126158666d; DESCRIPTION=Acute bronchitis (disorder); START=2009-01-08 |
| `immunizations` | immunizations | 13,189 | 5 | `DATE`, `PATIENT`, `ENCOUNTER`, `CODE`, `DESCRIPTION` | DATE=2008-03-11; PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=5114a5b4-64b8-47b2-82a6-0ce24aae0943; CODE=140 |
| `medications` | medications | 6,048 | 8 | `START`, `PATIENT`, `ENCOUNTER`, `CODE`, `STOP`, `DESCRIPTION`, `REASONCODE` | START=1988-09-05; PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=5114a5b4-64b8-47b2-82a6-0ce24aae0943; CODE=834060 |
| `observations` | observations | 78,899 | 7 | `PATIENT`, `ENCOUNTER`, `DATE`, `CODE`, `DESCRIPTION`, `VALUE`, `UNITS` | PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=5114a5b4-64b8-47b2-82a6-0ce24aae0943; DATE=2008-03-11; CODE=8302-2 |
| `procedures` | procedures | 10,184 | 7 | `PATIENT`, `ENCOUNTER`, `DATE`, `CODE`, `DESCRIPTION`, `REASONCODE`, `REASONDESCRIPTION` | PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=6f2e3935-b203-493e-a9c0-f23e847b9798; DATE=2013-02-09; CODE=23426006 |
| `claims` | claims | 20,523 | 7 | `ID`, `PATIENT`, `ENCOUNTER`, `BILLABLEPERIOD`, `ORGANIZATION`, `DIAGNOSIS`, `TOTAL` | ID=1a9e880e-27a1-4465-8adc-222b1996a14a; PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=71949668-1c2e-43ae-ab0a-64654608defb; BILLABLEPERIOD=2008-03-11 |

## genes

주요 테이블: Classification, Genes, Interactions

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Classification` | Classification | 862 | 2 | `GeneID`, `Localization` | GeneID=G234064; Localization=cytoplasm |
| `Genes` | Genes | 4,346 | 9 | `GeneID`, `Essential`, `Class`, `Complex`, `Phenotype`, `Motif`, `Chromosome` | GeneID=G234064; Essential=Essential; Class=GTP/GDP-exchange factors (GEFs); Complex=Translation complexes |
| `Interactions` | Interactions | 910 | 4 | `GeneID1`, `GeneID2`, `Type`, `Expression_Corr` | GeneID1=G234064; GeneID2=G234126; Type=Genetic-Physical; Expression_Corr=0.914095071 |

## retails

주요 테이블: customer, lineitem, nation, orders, part

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `customer` | customer | 150,000 | 8 | `c_custkey`, `c_nationkey`, `c_mktsegment`, `c_name`, `c_address`, `c_phone`, `c_acctbal` | c_custkey=1; c_nationkey=8; c_mktsegment=BUILDING; c_name=Customer#000000001 |
| `lineitem` | lineitem | 4,423,659 | 16 | `l_orderkey`, `l_linenumber`, `l_suppkey`, `l_partkey`, `l_shipdate`, `l_discount`, `l_extendedprice` | l_orderkey=1; l_linenumber=1; l_suppkey=6296; l_partkey=98768 |
| `nation` | nation | 25 | 4 | `n_nationkey`, `n_regionkey`, `n_name`, `n_comment` | n_nationkey=0; n_regionkey=0; n_name=ALGERIA; n_comment=slyly express pinto beans cajole idly. deposi... |
| `orders` | orders | 1,500,000 | 9 | `o_orderkey`, `o_custkey`, `o_orderdate`, `o_orderpriority`, `o_shippriority`, `o_clerk`, `o_orderstatus` | o_orderkey=1; o_custkey=73100; o_orderdate=1995-04-19; o_orderpriority=4-NOT SPECIFIED |
| `part` | part | 200,000 | 9 | `p_partkey`, `p_type`, `p_size`, `p_brand`, `p_name`, `p_container`, `p_mfgr` | p_partkey=1; p_type=LARGE PLATED TIN; p_size=31; p_brand=Brand#43 |
| `partsupp` | partsupp | 800,000 | 5 | `ps_partkey`, `ps_suppkey`, `ps_supplycost`, `ps_availqty`, `ps_comment` | ps_partkey=1; ps_suppkey=2; ps_supplycost=400.75; ps_availqty=1111 |
| `region` | region | 5 | 3 | `r_regionkey`, `r_name`, `r_comment` | r_regionkey=0; r_name=AFRICA; r_comment=asymptotes sublate after the r |
| `supplier` | supplier | 10,000 | 7 | `s_suppkey`, `s_nationkey`, `s_comment`, `s_name`, `s_address`, `s_phone`, `s_acctbal` | s_suppkey=1; s_nationkey=13; s_comment=blithely final pearls are. instructions thra; s_name=Supplier#000000001 |

## talkingdata

주요 테이블: app_all, app_events, app_events_relevant, app_labels, events

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `app_all` | app_all | 113,211 | 1 | `app_id` | app_id=-9223281467940916832 |
| `app_events` | app_events | 32,473,067 | 4 | `event_id`, `app_id`, `is_installed`, `is_active` | event_id=2; app_id=-8942695423876075857; is_installed=1; is_active=0 |
| `app_events_relevant` | app_events_relevant | 3,701,900 | 4 | `event_id`, `app_id`, `is_installed`, `is_active` | event_id=2; app_id=-8942695423876075857; is_installed=1; is_active=0 |
| `app_labels` | app_labels | 459,943 | 2 | `app_id`, `label_id` | app_id=7324884708820027918; label_id=251 |
| `events` | events | 3,252,950 | 5 | `event_id`, `device_id`, `timestamp`, `longitude`, `latitude` | event_id=1; device_id=29182687948017175; timestamp=2016-05-01 00:55:25.0; longitude=121.0 |
| `events_relevant` | events_relevant | 167,389 | 5 | `event_id`, `device_id`, `timestamp`, `longitude`, `latitude` | event_id=2; device_id=NULL; timestamp=-8942695423876075857; longitude=1.0 |
| `gender_age` | gender_age | 186,697 | 4 | `device_id`, `gender`, `age`, `group` | device_id=-9221086586254644858; gender=M; age=29; group=M29-31 |
| `gender_age_test` | gender_age_test | 112,071 | 1 | `device_id` | device_id=-9223321966609553846 |
| `gender_age_train` | gender_age_train | 74,645 | 4 | `device_id`, `gender`, `age`, `group` | device_id=-9223067244542181226; gender=M; age=24; group=M23-26 |
| `label_categories` | label_categories | 930 | 2 | `label_id`, `category` | label_id=1; category=NULL |
| `phone_brand_device_model2` | phone_brand_device_model2 | 89,200 | 3 | `device_id`, `phone_brand`, `device_model` | device_id=-9223321966609553846; phone_brand=小米; device_model=红米note |
| `sample_submission` | sample_submission | 13,700 | 13 | `device_id`, `F23-`, `F24-26`, `F27-28`, `F29-32`, `F33-42`, `F43+` | device_id=-9223321966609553846; F23-=0.0833; F24-26=0.0833; F27-28=0.0833 |

## beer_factory

주요 테이블: customers, geolocation, location, rootbeerbrand, rootbeer

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `customers` | 고객 ID, 세그먼트, 통화 | 554 | 12 | `CustomerID`, `First`, `Last`, `StreetAddress`, `City`, `State`, `ZipCode` | CustomerID=101811; First=Kenneth; Last=Walton; StreetAddress=6715 Commonwealth Dr |
| `geolocation` | geolocation | 3 | 3 | `LocationID`, `Latitude`, `Longitude` | LocationID=0; Latitude=0.0; Longitude=0.0 |
| `location` | location | 3 | 6 | `LocationID`, `LocationName`, `StreetAddress`, `City`, `State`, `ZipCode` | LocationID=0; LocationName=LOST; StreetAddress=NULL; City=NULL |
| `rootbeerbrand` | rootbeerbrand | 24 | 22 | `BrandID`, `BrandName`, `FirstBrewedYear`, `BreweryName`, `City`, `State`, `Country` | BrandID=10001; BrandName=A&W; FirstBrewedYear=1919; BreweryName=Dr Pepper Snapple Group |
| `rootbeer` | rootbeer | 6,430 | 5 | `RootBeerID`, `BrandID`, `LocationID`, `ContainerType`, `PurchaseDate` | RootBeerID=100000; BrandID=10001; LocationID=1; ContainerType=Bottle |
| `rootbeerreview` | rootbeerreview | 713 | 5 | `CustomerID`, `BrandID`, `StarRating`, `ReviewDate`, `Review` | CustomerID=101811; BrandID=10012; StarRating=5; ReviewDate=2013-07-15 |
| `transaction` | transaction | 6,312 | 8 | `TransactionID`, `CustomerID`, `LocationID`, `RootBeerID`, `CreditCardNumber`, `TransactionDate`, `CreditCardType` | TransactionID=100000; CustomerID=864896; LocationID=2; RootBeerID=105661 |

## chicago_crime

주요 테이블: Community_Area, District, FBI_Code, IUCR, Neighborhood

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Community_Area` | Community_Area | 77 | 4 | `community_area_no`, `community_area_name`, `side`, `population` | community_area_no=1; community_area_name=Rogers Park; side=Far North ; population=54,991 |
| `District` | District | 22 | 10 | `district_no`, `district_name`, `address`, `zip_code`, `commander`, `email`, `phone` | district_no=1; district_name=Central; address=1718 South State Street; zip_code=60616 |
| `FBI_Code` | FBI_Code | 26 | 4 | `fbi_code_no`, `title`, `description`, `crime_against` | fbi_code_no=01A; title=Homicide 1st & 2nd Degree; description=The killing of one human being by another.; crime_against=Persons |
| `IUCR` | IUCR | 401 | 4 | `iucr_no`, `primary_description`, `secondary_description`, `index_code` | iucr_no=110; primary_description=HOMICIDE; secondary_description=FIRST DEGREE MURDER; index_code=I |
| `Neighborhood` | Neighborhood | 246 | 2 | `neighborhood_name`, `community_area_no` | neighborhood_name=Albany Park; community_area_no=14 |
| `Ward` | Ward | 50 | 13 | `ward_no`, `alderman_first_name`, `alderman_last_name`, `alderman_name_suffix`, `ward_office_address`, `ward_office_zip`, `ward_email` | ward_no=1; alderman_first_name=Daniel; alderman_last_name=La Spata; alderman_name_suffix=NULL |
| `Crime` | Crime | 268,002 | 15 | `report_no`, `iucr_no`, `district_no`, `ward_no`, `community_area_no`, `fbi_code_no`, `case_number` | report_no=23757; iucr_no=110; district_no=17; ward_no=30 |

## mondial_geo

주요 테이블: borders, city, continent, country, desert

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `borders` | borders | 320 | 3 | `Country1`, `Country2`, `Length` | Country1=A; Country2=CH; Length=164.0 |
| `city` | city | 3,111 | 6 | `Name`, `Province`, `Country`, `Population`, `Longitude`, `Latitude` | Name=Aachen; Province=Nordrhein Westfalen; Country=D; Population=247113 |
| `continent` | continent | 5 | 2 | `Name`, `Area` | Name=Africa; Area=30254700.0 |
| `country` | country | 238 | 6 | `Code`, `Name`, `Capital`, `Province`, `Area`, `Population` | Code=A; Name=Austria; Capital=Vienna; Province=Vienna |
| `desert` | desert | 63 | 4 | `Name`, `Area`, `Longitude`, `Latitude` | Name=Arabian Desert; Area=50000.0; Longitude=26.0; Latitude=33.0 |
| `economy` | economy | 238 | 6 | `Country`, `GDP`, `Agriculture`, `Service`, `Industry`, `Inflation` | Country=A; GDP=152000.0; Agriculture=2.0; Service=34.0 |
| `encompasses` | encompasses | 242 | 3 | `Country`, `Continent`, `Percentage` | Country=A; Continent=Europe; Percentage=100.0 |
| `ethnicGroup` | ethnicGroup | 540 | 3 | `Country`, `Name`, `Percentage` | Country=GE; Name=Abkhaz; Percentage=1.8 |
| `geo_desert` | geo_desert | 155 | 3 | `Desert`, `Country`, `Province` | Desert=Desert; Country=Coun; Province=Province |
| `geo_estuary` | geo_estuary | 266 | 3 | `River`, `Country`, `Province` | River=River; Country=Coun; Province=Province |
| `geo_island` | geo_island | 202 | 3 | `Island`, `Country`, `Province` | Island=Aland; Country=Alan; Province=650 |
| `geo_lake` | geo_lake | 254 | 3 | `Lake`, `Country`, `Province` | Lake=Lake; Country=Coun; Province=Province |
| `geo_mountain` | geo_mountain | 296 | 3 | `Mountain`, `Country`, `Province` | Mountain=Mountain; Country=Coun; Province=Province |
| `geo_river` | geo_river | 852 | 3 | `River`, `Country`, `Province` | River=River; Country=Coun; Province=Province |
| `geo_sea` | geo_sea | 736 | 3 | `Sea`, `Country`, `Province` | Sea=Sea; Country=Coun; Province=Province |
| `geo_source` | geo_source | 220 | 3 | `River`, `Country`, `Province` | River=River; Country=Coun; Province=Province |
| `island` | island | 276 | 7 | `Name`, `Islands`, `Area`, `Height`, `Type`, `Longitude`, `Latitude` | Name=Aland; Islands=Aland Islands; Area=650.0; Height=NULL |
| `islandIn` | islandIn | 350 | 4 | `Island`, `Sea`, `Lake`, `River` | Island=Island; Sea=Sea; Lake=Lake; River=River |
| `isMember` | isMember | 8,009 | 3 | `Country`, `Organization`, `Type` | Country=Coun; Organization=Organization; Type=Type |
| `lake` | lake | 130 | 8 | `Name`, `Area`, `Depth`, `Altitude`, `Type`, `River`, `Longitude` | Name=Ammersee; Area=46.6; Depth=81.1; Altitude=533.0 |
| `language` | language | 144 | 3 | `Country`, `Name`, `Percentage` | Country=AFG; Name=Afghan Persian; Percentage=50.0 |
| `located` | located | 858 | 6 | `City`, `Province`, `Country`, `River`, `Lake`, `Sea` | City=City; Province=Province; Country=Coun; River=River |
| `locatedOn` | locatedOn | 435 | 4 | `City`, `Province`, `Country`, `Island` | City=City; Province=Province; Country=Coun; Island=Island |
| `mergesWith` | mergeswith | 55 | 2 | `Sea1`, `Sea2` | Sea1=Sea1; Sea2=Sea2 |
| `mountain` | mountain | 0 | 6 | `Name`, `Mountains`, `Height`, `Type`, `Longitude`, `Latitude` | - |
| `mountainOnIsland` | mountainonisland | 68 | 2 | `Mountain`, `Island` | Mountain=Mountain; Island=Island |
| `organization` | organization | 154 | 6 | `Abbreviation`, `City`, `Country`, `Province`, `Name`, `Established` | Abbreviation=Abbreviation; City=City; Country=Coun; Province=Province |
| `politics` | politics | 239 | 4 | `Country`, `Dependent`, `Independence`, `Government` | Country=Coun; Dependent=Depe; Independence=Independence; Government=Government |
| `population` | population | 238 | 3 | `Country`, `Population_Growth`, `Infant_Mortality` | Country=A; Population_Growth=0.41; Infant_Mortality=6.2 |
| `province` | province | 1,450 | 6 | `Name`, `Country`, `Population`, `Area`, `Capital`, `CapProv` | Name=Aali an Nil; Country=SUD; Population=1599605; Area=238792.0 |
| `religion` | religion | 454 | 3 | `Country`, `Name`, `Percentage` | Country=BERM; Name=African Methodist Episcopal; Percentage=11.0 |
| `river` | river | 218 | 11 | `Name`, `Lake`, `River`, `Sea`, `Length`, `SourceLongitude`, `SourceLatitude` | Name=Aare; Lake=Brienzersee; River=Rhein; Sea=NULL |
| `sea` | sea | 35 | 2 | `Name`, `Depth` | Name=Andaman Sea; Depth=3113.0 |
| `target` | target | 205 | 2 | `Country`, `Target` | Country=Coun; Target=Target |

## student_loan

주요 테이블: bool, person, disabled, enlist, filed_for_bankrupcy

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `bool` | bool | 2 | 1 | `name` | name=neg |
| `person` | person | 1,000 | 1 | `name` | name=student1 |
| `disabled` | disabled | 95 | 1 | `name` | name=student114 |
| `enlist` | enlist | 306 | 2 | `name`, `organ` | name=student40; organ=fire_department |
| `filed_for_bankrupcy` | filed_for_bankruptcy | 96 | 1 | `name` | name=student122 |
| `longest_absense_from_school` | longest_absense_from_school | 1,000 | 2 | `name`, `month` | name=student10; month=0 |
| `male` | male | 497 | 1 | `name` | name=student1 |
| `no_payment_due` | no_payment_due | 1,000 | 2 | `name`, `bool` | name=student10; bool=neg |
| `unemployed` | unemployed | 98 | 1 | `name` | name=student1000 |
| `enrolled` | enrolled | 1,194 | 3 | `name`, `school`, `month` | name=student10; school=smc; month=1 |

## codebase_comments

주요 테이블: Method, MethodParameter, Repo, Solution

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Method` | Method | 3,508,215 | 10 | `Id`, `Name`, `FullComment`, `Summary`, `ApiCalls`, `CommentIsXml`, `SampledAt` | Id=1; Name=HtmlSharp.HtmlParser.Feed; FullComment=Feeds data into the parser; Summary=NULL |
| `MethodParameter` | MethodParameter | 5,132,027 | 4 | `Id`, `MethodId`, `Type`, `Name` | Id=1; MethodId=1; Type=System.String; Name=data |
| `Repo` | Repo | 140,990 | 6 | `Id`, `Url`, `Stars`, `Forks`, `Watchers`, `ProcessedTime` | Id=1; Url=https://github.com/wallerdev/htmlsharp.git; Stars=14; Forks=2 |
| `Solution` | Solution | 338,087 | 5 | `Id`, `RepoId`, `Path`, `ProcessedTime`, `WasCompiled` | Id=1; RepoId=1; Path=wallerdev_htmlsharp\HtmlSharp.sln; ProcessedTime=636430963695642191 |

## retail_world

주요 테이블: Categories, Customers, Employees, Shippers, Suppliers

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Categories` | Categories | 8 | 3 | `CategoryID`, `CategoryName`, `Description` | CategoryID=1; CategoryName=Beverages; Description=Soft drinks, coffees, teas, beers, and ales |
| `Customers` | Customers | 91 | 7 | `CustomerID`, `CustomerName`, `ContactName`, `Address`, `City`, `PostalCode`, `Country` | CustomerID=1; CustomerName=Alfreds Futterkiste; ContactName=Maria Anders; Address=Obere Str. 57 |
| `Employees` | Employees | 10 | 6 | `EmployeeID`, `LastName`, `FirstName`, `BirthDate`, `Photo`, `Notes` | EmployeeID=1; LastName=Davolio; FirstName=Nancy; BirthDate=1968-12-08 |
| `Shippers` | Shippers | 3 | 3 | `ShipperID`, `ShipperName`, `Phone` | ShipperID=1; ShipperName=Speedy Express; Phone=(503) 555-9831 |
| `Suppliers` | Suppliers | 29 | 8 | `SupplierID`, `SupplierName`, `ContactName`, `Address`, `City`, `PostalCode`, `Country` | SupplierID=1; SupplierName=Exotic Liquid; ContactName=Charlotte Cooper; Address=49 Gilbert St. |
| `Products` | Products | 77 | 6 | `ProductID`, `SupplierID`, `CategoryID`, `ProductName`, `Unit`, `Price` | ProductID=1; SupplierID=1; CategoryID=1; ProductName=Chais |
| `Orders` | Orders | 196 | 5 | `OrderID`, `CustomerID`, `EmployeeID`, `ShipperID`, `OrderDate` | OrderID=10248; CustomerID=90; EmployeeID=5; ShipperID=3 |
| `OrderDetails` | OrderDetails | 518 | 4 | `OrderDetailID`, `OrderID`, `ProductID`, `Quantity` | OrderDetailID=1; OrderID=10248; ProductID=11; Quantity=12 |

## music_tracker

주요 테이블: torrents, tags

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `torrents` | torrents | 75,719 | 7 | `id`, `groupName`, `totalSnatched`, `artist`, `groupYear`, `releaseType`, `groupId` | id=0; groupName=superappin&#39;; totalSnatched=239; artist=grandmaster flash & the furious five |
| `tags` | 태그 통계 | 161,283 | 3 | `index`, `id`, `tag` | index=0; id=0; tag=1970s |

## disney

주요 테이블: characters, director, movies_total_gross, revenue, voice-actors

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `characters` | characters | 56 | 5 | `movie_title`, `hero`, `release_date`, `villian`, `song` | movie_title=Snow White and the Seven Dwarfs; hero=Snow White; release_date=21-Dec-37; villian=Evil Queen |
| `director` | director | 56 | 2 | `name`, `director` | name=Snow White and the Seven Dwarfs; director=David Hand |
| `movies_total_gross` | movies_total_gross | 579 | 6 | `movie_title`, `release_date`, `genre`, `MPAA_rating`, `total_gross`, `inflation_adjusted_gross` | movie_title=Snow White and the Seven Dwarfs; release_date=Dec 21, 1937; genre=Musical; MPAA_rating=G |
| `revenue` | revenue | 26 | 7 | `Year`, `Studio Entertainment[NI 1]`, `Disney Consumer Products[NI 2]`, `Disney Interactive[NI 3][Rev 1]`, `Walt Disney Parks and Resorts`, `Disney Media Networks`, `Total` | Year=1991; Studio Entertainment[NI 1]=2593.0; Disney Consumer Products[NI 2]=724.0; Disney Interactive[NI 3][Rev 1]=NULL |
| `voice-actors` | voice-actors | 922 | 3 | `character`, `movie`, `voice-actor` | character=Abby Mallard; movie=Chicken Little; voice-actor=Joan Cusack |

## college_completion

주요 테이블: institution_details, institution_grads, state_sector_grads, state_sector_details

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `institution_details` | institution_details | 3,798 | 62 | `unitid`, `chronname`, `city`, `state`, `level`, `control`, `basic` | unitid=100654; chronname=Alabama A&M University; city=Normal; state=Alabama |
| `institution_grads` | institution_grads | 1,302,102 | 10 | `unitid`, `year`, `gender`, `race`, `cohort`, `grad_cohort`, `grad_100` | unitid=100760; year=2011; gender=B; race=X |
| `state_sector_grads` | state_sector_grads | 84,942 | 15 | `stateid`, `state`, `state_abbr`, `control`, `level`, `year`, `gender` | stateid=1; state=Alabama; state_abbr=AL; control=Private for-profit |
| `state_sector_details` | state_sector_details | 312 | 15 | `stateid`, `level`, `control`, `state`, `state_post`, `schools_count`, `counted_pct` | stateid=0; level=4-year; control=Public; state=United States |

## ice_hockey_draft

주요 테이블: height_info, weight_info, PlayerInfo, SeasonStatus

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `height_info` | height_info | 16 | 3 | `height_id`, `height_in_cm`, `height_in_inch` | height_id=65; height_in_cm=165; height_in_inch=5'5" |
| `weight_info` | weight_info | 46 | 3 | `weight_id`, `weight_in_kg`, `weight_in_lbs` | weight_id=154; weight_in_kg=70; weight_in_lbs=154 |
| `PlayerInfo` | PlayerInfo | 2,171 | 20 | `ELITEID`, `height`, `weight`, `PlayerName`, `birthdate`, `birthyear`, `birthmonth` | ELITEID=9; height=73; weight=198; PlayerName=David Bornhammar |
| `SeasonStatus` | SeasonStatus | 5,485 | 11 | `ELITEID`, `SEASON`, `TEAM`, `LEAGUE`, `GAMETYPE`, `GP`, `G` | ELITEID=3667; SEASON=1997-1998; TEAM=Rimouski Oceanic; LEAGUE=QMJHL |

## world_development_indicators

주요 테이블: Country, Series, CountryNotes, Footnotes, Indicators

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Country` | 국가 | 247 | 31 | `CountryCode`, `ShortName`, `TableName`, `LongName`, `Alpha2Code`, `CurrencyUnit`, `SpecialNotes` | CountryCode=AFG; ShortName=Afghanistan; TableName=Afghanistan; LongName=Islamic State of Afghanistan |
| `Series` | Series | 1,345 | 20 | `SeriesCode`, `Topic`, `IndicatorName`, `ShortDefinition`, `LongDefinition`, `UnitOfMeasure`, `Periodicity` | SeriesCode=BN.KLT.DINV.CD; Topic=Economic Policy & Debt: Balance of payments: ...; IndicatorName=Foreign direct investment, net (BoP, current ...; ShortDefinition= |
| `CountryNotes` | CountryNotes | 4,857 | 3 | `Countrycode`, `Seriescode`, `Description` | Countrycode=ABW; Seriescode=EG.EGY.PRIM.PP.KD; Description=Sources: Estimated based on UN Energy Statist... |
| `Footnotes` | FootNotes | 532,415 | 4 | `Countrycode`, `Seriescode`, `Year`, `Description` | Countrycode=ABW; Seriescode=AG.LND.FRST.K2; Year=YR1990; Description=Not specified |
| `Indicators` | Indicators | 5,656,458 | 6 | `CountryCode`, `IndicatorCode`, `Year`, `CountryName`, `IndicatorName`, `Value` | CountryCode=ARB; IndicatorCode=SP.ADO.TFRT; Year=1960; CountryName=Arab World |
| `SeriesNotes` | SeriesNotes | 369 | 3 | `Seriescode`, `Year`, `Description` | Seriescode=SP.ADO.TFRT; Year=YR1960; Description=Interpolated using data for 1957 and 1962. |

## airline

주요 테이블: Air Carriers, Airports, Airlines

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Air Carriers` | Air Carriers | 1,656 | 2 | `Code`, `Description` | Code=19031; Description=Mackey International Inc.: MAC |
| `Airports` | Airports | 6,510 | 2 | `Code`, `Description` | Code=01A; Description=Afognak Lake, AK: Afognak Lake Airport |
| `Airlines` | Airlines | 701,352 | 28 | `OP_CARRIER_AIRLINE_ID`, `ORIGIN`, `DEST`, `FL_DATE`, `TAIL_NUM`, `OP_CARRIER_FL_NUM`, `ORIGIN_AIRPORT_ID` | OP_CARRIER_AIRLINE_ID=19805; ORIGIN=JFK; DEST=PHX; FL_DATE=2018/8/1 |

## retail_complains

주요 테이블: state, callcenterlogs, client, district, events

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `state` | state | 48 | 3 | `StateCode`, `State`, `Region` | StateCode=AL; State=Alabama; Region=South |
| `callcenterlogs` | callcenterlogs | 3,999 | 13 | `Complaint ID`, `rand client`, `Date received`, `phonefinal`, `vru+line`, `call_id`, `priority` | Complaint ID=CR2406263; rand client=C00004587; Date received=2017-03-27; phonefinal=977-806-9726 |
| `client` | 은행 고객과 지역 정보 | 5,369 | 18 | `client_id`, `district_id`, `sex`, `day`, `month`, `year`, `age` | client_id=C00000001; district_id=18; sex=Female; day=13 |
| `district` | 지역 인구·경제 통계 | 77 | 4 | `district_id`, `state_abbrev`, `city`, `division` | district_id=1; state_abbrev=NY; city=New York City; division=Middle Atlantic |
| `events` | events | 23,419 | 15 | `Complaint ID`, `Client_ID`, `Date received`, `Product`, `Sub-product`, `Issue`, `Sub-issue` | Complaint ID=CR0922485; Client_ID=C00001925; Date received=2014-07-03; Product=Bank account or service |
| `reviews` | reviews | 377 | 5 | `Date`, `district_id`, `Stars`, `Reviews`, `Product` | Date=2017-10-04; district_id=65; Stars=5; Reviews=Great job, Eagle National! Each person was pr... |

## trains

주요 테이블: cars, trains

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `cars` | cars | 63 | 10 | `id`, `train_id`, `position`, `shape`, `len`, `sides`, `roof` | id=1; train_id=1; position=1; shape=rectangle |
| `trains` | trains | 20 | 2 | `id`, `direction` | id=1; direction=east |

## public_review_platform

주요 테이블: Attributes, Categories, Compliments, Days, Years

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Attributes` | Attributes | 80 | 2 | `attribute_id`, `attribute_name` | attribute_id=1; attribute_name=Alcohol |
| `Categories` | Categories | 591 | 2 | `category_id`, `category_name` | category_id=1; category_name=Active Life |
| `Compliments` | Compliments | 11 | 2 | `compliment_id`, `compliment_type` | compliment_id=1; compliment_type=photos |
| `Days` | Days | 7 | 2 | `day_id`, `day_of_week` | day_id=1; day_of_week=Sunday |
| `Years` | Years | 10 | 2 | `year_id`, `actual_year` | year_id=2005; actual_year=2005 |
| `Business_Attributes` | Business_Attributes | 206,934 | 3 | `attribute_id`, `business_id`, `attribute_value` | attribute_id=1; business_id=2; attribute_value=none |
| `Business_Categories` | Business_Categories | 43,703 | 2 | `business_id`, `category_id` | business_id=1; category_id=8 |
| `Business_Hours` | Business_Hours | 47,831 | 4 | `business_id`, `day_id`, `opening_time`, `closing_time` | business_id=2; day_id=2; opening_time=11AM; closing_time=8PM |
| `Checkins` | Checkins | 80,038 | 26 | `business_id`, `day_id`, `label_time_0`, `label_time_1`, `label_time_2`, `label_time_3`, `label_time_4` | business_id=1; day_id=1; label_time_0=None; label_time_1=None |
| `Elite` | Elite | 16,366 | 2 | `user_id`, `year_id` | user_id=3; year_id=2010 |
| `Reviews` | Reviews | 322,906 | 7 | `business_id`, `user_id`, `review_stars`, `review_votes_funny`, `review_votes_useful`, `review_votes_cool`, `review_length` | business_id=1; user_id=36129; review_stars=2; review_votes_funny=None |
| `Tips` | Tips | 87,157 | 4 | `business_id`, `user_id`, `likes`, `tip_length` | business_id=2; user_id=12490; likes=0; tip_length=Medium |
| `Users_Compliments` | Users_Compliments | 98,810 | 3 | `compliment_id`, `user_id`, `number_of_compliments` | compliment_id=1; user_id=3; number_of_compliments=Medium |
| `Business` | Business | 15,585 | 6 | `business_id`, `active`, `city`, `state`, `stars`, `review_count` | business_id=1; active=true; city=Phoenix; state=AZ |
| `Users` | Users | 70,817 | 8 | `user_id`, `user_yelping_since_year`, `user_average_stars`, `user_votes_funny`, `user_votes_useful`, `user_votes_cool`, `user_review_count` | user_id=1; user_yelping_since_year=2012; user_average_stars=4.0; user_votes_funny=Low |

## donor

주요 테이블: essays, projects, donations, resources

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `essays` | essays | 99,998 | 6 | `projectid`, `teacher_acctid`, `title`, `short_description`, `need_statement`, `essay` | projectid=ffffc4f85b60efc5b52347df489d0238; teacher_acctid=c24011b20fc161ed02248e85beb59a90; title=iMath; short_description=It is imperative that teachers bring technolo... |
| `projects` | projects | 664,098 | 35 | `projectid`, `teacher_acctid`, `schoolid`, `school_ncesid`, `school_latitude`, `school_longitude`, `school_city` | projectid=316ed8fb3b81402ff6ac8f721bb31192; teacher_acctid=42d43fa6f37314365d08692e08680973; schoolid=c0e6ce89b244764085691a1b8e28cb81; school_ncesid=063627006187 |
| `donations` | donations | 3,097,556 | 21 | `donationid`, `projectid`, `donor_acctid`, `donor_city`, `donor_state`, `donor_zip`, `is_teacher_acct` | donationid=431d720bc3dfd75ae445a5eaa0b0638d; projectid=ffffac55ee02a49d1abc87ba6fc61135; donor_acctid=22cbc920c9b5fa08dfb331422f5926b5; donor_city=Peachtree City |
| `resources` | resources | 3,666,757 | 9 | `resourceid`, `projectid`, `vendorid`, `vendor_name`, `project_resource_type`, `item_name`, `item_number` | resourceid=8a1c1c45bc30d065061912fd9114fcf3; projectid=ffffc4f85b60efc5b52347df489d0238; vendorid=430; vendor_name=Woodwind and Brasswind |

## coinmarketcap

주요 테이블: coins, historical

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `coins` | Coins | 8,927 | 15 | `id`, `name`, `slug`, `symbol`, `status`, `category`, `description` | id=1; name=Bitcoin; slug=bitcoin; symbol=BTC |
| `historical` | Historical | 4,441,972 | 19 | `date`, `coin_id`, `cmc_rank`, `market_cap`, `price`, `open`, `high` | date=2013-04-28; coin_id=1; cmc_rank=1; market_cap=1488566971.9558687 |

## simpson_episodes

주요 테이블: Episode, Person, Award, Character_Award, Credit

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Episode` | Episode | 21 | 10 | `episode_id`, `season`, `episode`, `number_in_series`, `title`, `summary`, `air_date` | episode_id=S20-E1; season=20; episode=1; number_in_series=421 |
| `Person` | Person | 369 | 8 | `name`, `birthdate`, `birth_name`, `birth_place`, `birth_region`, `birth_country`, `height_meters` | name=Marc Wilmore; birthdate=1963-05-04; birth_name=Marc Edward Wilmore; birth_place=NULL |
| `Award` | Award | 75 | 11 | `award_id`, `person`, `episode_id`, `organization`, `year`, `award_category`, `award` | award_id=325; person=Dan Castellaneta; episode_id=S20-E18; organization=Primetime Emmy Awards |
| `Character_Award` | Character_Award | 12 | 2 | `award_id`, `character` | award_id=325; character=Homer Simpson |
| `Credit` | Credit | 4,557 | 5 | `episode_id`, `person`, `category`, `role`, `credited` | episode_id=S20-E10; person=Bonita Pietila; category=Casting Department; role=casting |
| `Keyword` | Keyword | 307 | 2 | `episode_id`, `keyword` | episode_id=S20-E1; keyword=1930s to 2020s |
| `Vote` | Vote | 210 | 4 | `episode_id`, `stars`, `votes`, `percent` | episode_id=S20-E1; stars=2; votes=16; percent=1.3 |

## movie_3

주요 테이블: film_text, actor, address, category, city

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `film_text` | film_text | 1,000 | 3 | `film_id`, `title`, `description` | film_id=1; title=ACADEMY DINOSAUR; description=A Epic Drama of a Feminist And a Mad Scientis... |
| `actor` | actor | 200 | 4 | `actor_id`, `first_name`, `last_name`, `last_update` | actor_id=1; first_name=PENELOPE; last_name=GUINESS; last_update=2006-02-15 04:34:33.0 |
| `address` | address | 603 | 8 | `address_id`, `city_id`, `address`, `address2`, `district`, `postal_code`, `phone` | address_id=1; city_id=300; address=47 MySakila Drive; address2=NULL |
| `category` | category | 16 | 3 | `category_id`, `name`, `last_update` | category_id=1; name=Action; last_update=2006-02-15 04:46:27.0 |
| `city` | city | 600 | 4 | `city_id`, `country_id`, `city`, `last_update` | city_id=1; country_id=87; city=A Corua (La Corua); last_update=2006-02-15 04:45:25.0 |
| `country` | country | 109 | 3 | `country_id`, `country`, `last_update` | country_id=1; country=Afghanistan; last_update=2006-02-15 04:44:00.0 |
| `customer` | customer | 599 | 9 | `customer_id`, `store_id`, `address_id`, `first_name`, `last_name`, `email`, `active` | customer_id=1; store_id=1; address_id=5; first_name=MARY |
| `film` | film | 1,000 | 13 | `film_id`, `language_id`, `original_language_id`, `title`, `description`, `release_year`, `rental_duration` | film_id=1; language_id=1; original_language_id=NULL; title=ACADEMY DINOSAUR |
| `film_actor` | film_actor | 5,462 | 3 | `actor_id`, `film_id`, `last_update` | actor_id=1; film_id=1; last_update=2006-02-15 05:05:03.0 |
| `film_category` | film_category | 1,000 | 3 | `film_id`, `category_id`, `last_update` | film_id=1; category_id=6; last_update=2006-02-15 05:07:09.0 |
| `inventory` | inventory | 4,581 | 4 | `inventory_id`, `film_id`, `store_id`, `last_update` | inventory_id=1; film_id=1; store_id=1; last_update=2006-02-15 05:09:17.0 |
| `language` | language | 6 | 3 | `language_id`, `name`, `last_update` | language_id=1; name=English; last_update=2006-02-15 05:02:19.0 |
| `payment` | payment | 16,049 | 7 | `payment_id`, `customer_id`, `staff_id`, `rental_id`, `amount`, `payment_date`, `last_update` | payment_id=1; customer_id=1; staff_id=1; rental_id=76 |
| `rental` | rental | 16,044 | 7 | `rental_id`, `inventory_id`, `customer_id`, `staff_id`, `rental_date`, `return_date`, `last_update` | rental_id=1; inventory_id=367; customer_id=130; staff_id=1 |
| `staff` | staff | 2 | 11 | `staff_id`, `address_id`, `store_id`, `first_name`, `last_name`, `picture`, `email` | staff_id=1; address_id=3; store_id=1; first_name=Mike |
| `store` | store | 2 | 4 | `store_id`, `manager_staff_id`, `address_id`, `last_update` | store_id=1; manager_staff_id=1; address_id=1; last_update=2006-02-15 04:57:12.0 |

## shooting

주요 테이블: incidents, officers, subjects

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `incidents` | Incidents | 219 | 8 | `case_number`, `date`, `location`, `subject_statuses`, `subject_weapon`, `subjects`, `subject_count` | case_number=031347-2015; date=2015/2/9; location=7400 Bonnie View Road; subject_statuses=Deceased |
| `officers` | officers | 370 | 6 | `case_number`, `race`, `gender`, `last_name`, `first_name`, `full_name` | case_number=44523A; race=L; gender=M; last_name=Patino |
| `subjects` | subjects | 223 | 6 | `case_number`, `race`, `gender`, `last_name`, `first_name`, `full_name` | case_number=44523A; race=L; gender=M; last_name=Curry |

## superstore

주요 테이블: people, product, central_superstore, east_superstore, south_superstore

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `people` | people | 2,501 | 8 | `Customer ID`, `Region`, `Customer Name`, `Segment`, `Country`, `City`, `State` | Customer ID=AA-10315; Region=Central; Customer Name=Alex Avila; Segment=Consumer |
| `product` | product | 5,298 | 5 | `Product ID`, `Region`, `Product Name`, `Category`, `Sub-Category` | Product ID=FUR-BO-10000330; Region=West; Product Name=Sauder Camden County Barrister Bookcase, Plan...; Category=Furniture |
| `central_superstore` | central_superstore | 4,646 | 12 | `Row ID`, `Customer ID`, `Region`, `Product ID`, `Order ID`, `Order Date`, `Ship Date` | Row ID=1; Customer ID=DP-13000; Region=Central; Product ID=OFF-PA-10000174 |
| `east_superstore` | east_superstore | 5,696 | 12 | `Row ID`, `Customer ID`, `Region`, `Product ID`, `Order ID`, `Order Date`, `Ship Date` | Row ID=4647; Customer ID=MB-18085; Region=East; Product ID=OFF-AR-10003478 |
| `south_superstore` | south_superstore | 3,240 | 12 | `Row ID`, `Customer ID`, `Region`, `Product ID`, `Order ID`, `Order Date`, `Ship Date` | Row ID=10343; Customer ID=JO-15145; Region=South; Product ID=OFF-AR-10002399 |
| `west_superstore` | west_superstore | 6,406 | 12 | `Row ID`, `Customer ID`, `Region`, `Product ID`, `Order ID`, `Order Date`, `Ship Date` | Row ID=13583; Customer ID=LS-17230; Region=West; Product ID=OFF-PA-10002005 |

## movielens

주요 테이블: users, directors, actors, movies, movies2actors

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `users` | 커뮤니티 사용자 | 6,039 | 4 | `userid`, `age`, `u_gender`, `occupation` | userid=1; age=1; u_gender=F; occupation=2 |
| `directors` | directors | 2,201 | 3 | `directorid`, `d_quality`, `avg_revenue` | directorid=67; d_quality=4; avg_revenue=1 |
| `actors` | actors | 98,690 | 3 | `actorid`, `a_gender`, `a_quality` | actorid=4; a_gender=M; a_quality=4 |
| `movies` | movies | 3,832 | 5 | `movieid`, `year`, `isEnglish`, `country`, `runningtime` | movieid=1672052; year=3; isEnglish=T; country=other |
| `movies2actors` | movies2actors | 138,349 | 3 | `movieid`, `actorid`, `cast_num` | movieid=1672580; actorid=981535; cast_num=0 |
| `movies2directors` | movies2directors | 4,141 | 3 | `movieid`, `directorid`, `genre` | movieid=1672111; directorid=54934; genre=Action |
| `u2base` | u2base | 996,159 | 3 | `userid`, `movieid`, `rating` | userid=2; movieid=1964242; rating=1 |
