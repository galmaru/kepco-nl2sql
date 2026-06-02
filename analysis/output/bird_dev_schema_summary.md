# BIRD Schema Summary

- Source: `BIRD dev_20240627`
- Databases: 11
- Tables: 75
- Columns: 798
- Questions: 1534
- Table samples: 3 rows per table

## Database Overview

| DB | 내용 | Tables | Columns | Rows | Questions |
|---|---|---:|---:|---:|---:|
| `debit_card_specializing` | 주유소 카드 결제 고객, 상품, 거래, 월별 소비 데이터 | 5 | 21 | 423,050 | 64 |
| `financial` | 은행 계좌, 고객, 카드, 대출, 거래, 지역 데이터 | 8 | 55 | 1,079,680 | 106 |
| `formula_1` | F1 경기, 드라이버, 팀, 서킷, 결과, 순위 데이터 | 13 | 94 | 514,287 | 174 |
| `california_schools` | 캘리포니아 학교, 급식 지원, SAT 성적 데이터 | 3 | 89 | 29,941 | 89 |
| `card_games` | 카드 게임 카드, 세트, 룰링, 언어별 카드 정보 데이터 | 6 | 115 | 803,445 | 191 |
| `european_football_2` | 유럽 축구 리그, 경기, 팀, 선수, 속성 데이터 | 7 | 199 | 222,796 | 129 |
| `thrombosis_prediction` | 혈전증 환자, 검사, 실험실 수치 데이터 | 3 | 64 | 15,952 | 163 |
| `toxicology` | 분자 독성, 원자, 결합, 연결 관계 데이터 | 4 | 11 | 49,813 | 145 |
| `student_club` | 학생 동아리 회원, 행사, 예산, 수입, 지출 데이터 | 8 | 48 | 42,511 | 158 |
| `superhero` | 슈퍼히어로 인물, 능력, 속성, 출판사, 정렬 데이터 | 10 | 31 | 10,614 | 129 |
| `codebase_community` | 개발자 커뮤니티 게시글, 댓글, 투표, 태그, 사용자 데이터 | 8 | 71 | 740,646 | 186 |

## debit_card_specializing

주유소 카드 결제 고객, 상품, 거래, 월별 소비 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `customers` | 고객 ID, 세그먼트, 통화 | 32,461 | 3 | `CustomerID`, `Segment`, `Currency` | CustomerID=3; Segment=SME; Currency=EUR |
| `gasstations` | 주유소와 체인, 국가, 세그먼트 | 5,716 | 4 | `GasStationID`, `ChainID`, `Country`, `Segment` | GasStationID=44; ChainID=13; Country=CZE; Segment=Value for money |
| `products` | 상품 ID와 설명 | 591 | 2 | `ProductID`, `Description` | ProductID=1; Description=Rucní zadání |
| `transactions_1k` | 카드 거래 일시, 고객, 주유소, 상품, 금액 | 1,000 | 9 | `TransactionID`, `Date`, `Time`, `CustomerID`, `CardID`, `GasStationID`, `ProductID` | TransactionID=1; Date=2012-08-24; Time=09:41:00; CustomerID=31543 |
| `yearmonth` | 고객별 월별 소비량 | 383,282 | 3 | `CustomerID`, `Date`, `Consumption` | CustomerID=5; Date=201207; Consumption=528.3 |

## financial

은행 계좌, 고객, 카드, 대출, 거래, 지역 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `account` | 은행 계좌, 개설 지점, 빈도, 날짜 | 4,500 | 4 | `account_id`, `district_id`, `frequency`, `date` | account_id=1; district_id=18; frequency=POPLATEK MESICNE; date=1995-03-24 |
| `card` | 카드 발급 정보 | 892 | 4 | `card_id`, `disp_id`, `type`, `issued` | card_id=1; disp_id=9; type=gold; issued=1998-10-16 |
| `client` | 은행 고객과 지역 정보 | 5,369 | 4 | `client_id`, `district_id`, `gender`, `birth_date` | client_id=1; district_id=18; gender=F; birth_date=1970-12-13 |
| `disp` | 계좌-고객 관계와 권한 유형 | 5,369 | 4 | `disp_id`, `client_id`, `account_id`, `type` | disp_id=1; client_id=1; account_id=1; type=OWNER |
| `district` | 지역 인구·경제 통계 | 77 | 16 | `district_id`, `A2`, `A3`, `A4`, `A5`, `A6`, `A7` | district_id=1; A2=Hl.m. Praha; A3=Prague; A4=1204953 |
| `loan` | 대출 금액, 기간, 상환, 상태 | 682 | 7 | `loan_id`, `account_id`, `date`, `amount`, `duration`, `payments`, `status` | loan_id=4959; account_id=2; date=1994-01-05; amount=80952 |
| `order` | 계좌 이체 주문 정보 | 6,471 | 6 | `order_id`, `account_id`, `bank_to`, `account_to`, `amount`, `k_symbol` | order_id=29401; account_id=1; bank_to=YZ; account_to=87144583 |
| `trans` | 계좌 거래 내역 | 1,056,320 | 10 | `trans_id`, `account_id`, `date`, `type`, `operation`, `amount`, `balance` | trans_id=1; account_id=1; date=1995-03-24; type=PRIJEM |

## formula_1

F1 경기, 드라이버, 팀, 서킷, 결과, 순위 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `circuits` | F1 서킷 위치와 속성 | 72 | 9 | `circuitId`, `circuitRef`, `name`, `location`, `country`, `lat`, `lng` | circuitId=2; circuitRef=sepang; name=Sepang International Circuit; location=Kuala Lumpur |
| `constructors` | F1 팀/제조사 | 208 | 5 | `constructorId`, `constructorRef`, `name`, `nationality`, `url` | constructorId=1; constructorRef=mclaren; name=McLaren; nationality=British |
| `drivers` | 드라이버 개인 정보 | 840 | 9 | `driverId`, `driverRef`, `number`, `code`, `forename`, `surname`, `dob` | driverId=1; driverRef=hamilton; number=44; code=HAM |
| `seasons` | 시즌 정보 | 68 | 2 | `year`, `url` | year=1950; url=http://en.wikipedia.org/wiki/1950_Formula_One... |
| `races` | 경기 일정과 서킷 | 976 | 8 | `raceId`, `year`, `circuitId`, `round`, `name`, `date`, `time` | raceId=1; year=2009; circuitId=1; round=1 |
| `constructorResults` | 팀별 경기 결과 | 11,082 | 5 | `constructorResultsId`, `raceId`, `constructorId`, `points`, `status` | constructorResultsId=1; raceId=18; constructorId=1; points=14.0 |
| `constructorStandings` | 팀별 시즌 순위 | 11,836 | 7 | `constructorStandingsId`, `raceId`, `constructorId`, `points`, `position`, `positionText`, `wins` | constructorStandingsId=1; raceId=18; constructorId=1; points=14.0 |
| `driverStandings` | 드라이버 시즌 순위 | 31,578 | 7 | `driverStandingsId`, `raceId`, `driverId`, `points`, `position`, `positionText`, `wins` | driverStandingsId=1; raceId=18; driverId=1; points=10.0 |
| `lapTimes` | 랩별 시간 기록 | 420,369 | 6 | `raceId`, `driverId`, `lap`, `position`, `time`, `milliseconds` | raceId=1; driverId=1; lap=1; position=13 |
| `pitStops` | 피트스톱 기록 | 6,070 | 7 | `raceId`, `driverId`, `stop`, `lap`, `time`, `duration`, `milliseconds` | raceId=841; driverId=1; stop=1; lap=16 |
| `qualifying` | 예선 결과 | 7,397 | 9 | `qualifyId`, `raceId`, `driverId`, `constructorId`, `number`, `position`, `q1` | qualifyId=1; raceId=18; driverId=1; constructorId=1 |
| `status` | 결과 상태 코드 | 134 | 2 | `statusId`, `status` | statusId=1; status=Finished |
| `results` | 레이스 최종 결과 | 23,657 | 18 | `resultId`, `raceId`, `driverId`, `constructorId`, `statusId`, `number`, `grid` | resultId=1; raceId=18; driverId=1; constructorId=1 |

## california_schools

캘리포니아 학교, 급식 지원, SAT 성적 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `frpm` | 무료/감면 급식 및 재학생 통계 | 9,986 | 29 | `CDSCode`, `Academic Year`, `County Code`, `District Code`, `School Code`, `County Name`, `District Name` | CDSCode=01100170109835; Academic Year=2014-2015; County Code=01; District Code=10017 |
| `satscores` | 학교별 SAT 응시 및 점수 | 2,269 | 11 | `cds`, `rtype`, `sname`, `dname`, `cname`, `enroll12`, `NumTstTakr` | cds=1100170000000; rtype=D; sname=NULL; dname=Alameda County Office of Education |
| `schools` | 학교 기본 정보와 위치/연락처 | 17,686 | 49 | `CDSCode`, `NCESDist`, `NCESSchool`, `StatusType`, `County`, `District`, `School` | CDSCode=01100170000000; NCESDist=0691051; NCESSchool=NULL; StatusType=Active |

## card_games

카드 게임 카드, 세트, 룰링, 언어별 카드 정보 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `cards` | 카드 속성, 색상, 타입, 가격 등 | 56,822 | 74 | `id`, `artist`, `asciiName`, `availability`, `borderColor`, `cardKingdomFoilId`, `cardKingdomId` | id=1; artist=Pete Venters; asciiName=NULL; availability=mtgo,paper |
| `foreign_data` | 언어별 카드명/텍스트 | 229,186 | 8 | `id`, `uuid`, `flavorText`, `language`, `multiverseid`, `name`, `text` | id=1; uuid=5f8287b1-5bb6-5f4c-ad17-316a40d5bb0c; flavorText=„Es ist der Wille aller, und meine Hand, die ...; language=German |
| `legalities` | 포맷별 카드 사용 가능 여부 | 427,907 | 4 | `id`, `uuid`, `format`, `status` | id=1; uuid=5f8287b1-5bb6-5f4c-ad17-316a40d5bb0c; format=commander; status=Legal |
| `sets` | 카드 세트 메타데이터 | 551 | 21 | `id`, `baseSetSize`, `block`, `booster`, `code`, `isFoilOnly`, `isForeignOnly` | id=1; baseSetSize=383; block=Core Set; booster={'default': {'boosters': [{'contents': {'basi... |
| `set_translations` | 카드 세트 번역명 | 1,210 | 4 | `id`, `setCode`, `language`, `translation` | id=1; setCode=10E; language=Chinese Simplified; translation=核心系列第十版 |
| `rulings` | 카드 룰 판정 기록 | 87,769 | 4 | `id`, `uuid`, `date`, `text` | id=1; uuid=6d268c95-c176-5766-9a46-c14f739aba1c; date=2007-07-15; text=You draw the card when Bandage resolves, not ... |

## european_football_2

유럽 축구 리그, 경기, 팀, 선수, 속성 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Player_Attributes` | 선수 능력치 시계열 | 183,978 | 42 | `id`, `player_fifa_api_id`, `player_api_id`, `date`, `overall_rating`, `potential`, `preferred_foot` | id=1; player_fifa_api_id=218353; player_api_id=505942; date=2016-02-18 00:00:00 |
| `Player` | 선수 기본 정보 | 11,060 | 7 | `id`, `player_api_id`, `player_name`, `player_fifa_api_id`, `birthday`, `height`, `weight` | id=1; player_api_id=505942; player_name=Aaron Appindangoye; player_fifa_api_id=218353 |
| `League` | 리그 | 11 | 3 | `id`, `country_id`, `name` | id=1; country_id=1; name=Belgium Jupiler League |
| `Country` | 국가 | 11 | 2 | `id`, `name` | id=1; name=Belgium |
| `Team` | 팀 기본 정보 | 299 | 5 | `id`, `team_api_id`, `team_fifa_api_id`, `team_long_name`, `team_short_name` | id=1; team_api_id=9987; team_fifa_api_id=673; team_long_name=KRC Genk |
| `Team_Attributes` | 팀 전술/능력치 시계열 | 1,458 | 25 | `id`, `team_fifa_api_id`, `team_api_id`, `date`, `buildUpPlaySpeed`, `buildUpPlaySpeedClass`, `buildUpPlayDribbling` | id=1; team_fifa_api_id=434; team_api_id=9930; date=2010-02-22 00:00:00 |
| `Match` | 축구 경기 상세 통계 | 25,979 | 115 | `id`, `country_id`, `league_id`, `home_team_api_id`, `away_team_api_id`, `home_player_1`, `home_player_2` | id=1; country_id=1; league_id=1; home_team_api_id=9987 |

## thrombosis_prediction

혈전증 환자, 검사, 실험실 수치 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `Examination` | 환자 진찰/진단 정보 | 806 | 13 | `ID`, `Examination Date`, `aCL IgG`, `aCL IgM`, `ANA`, `ANA Pattern`, `aCL IgA` | ID=14872; Examination Date=1997-05-27; aCL IgG=1.3; aCL IgM=1.6 |
| `Patient` | 환자 기본 정보 | 1,238 | 7 | `ID`, `SEX`, `Birthday`, `Description`, `First Date`, `Admission`, `Diagnosis` | ID=2110; SEX=F; Birthday=1934-02-13; Description=1994-02-14 |
| `Laboratory` | 실험실 검사 수치 | 13,908 | 44 | `ID`, `Date`, `GOT`, `GPT`, `LDH`, `ALP`, `TP` | ID=27654; Date=1991-09-11; GOT=34; GPT=36 |

## toxicology

분자 독성, 원자, 결합, 연결 관계 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `atom` | 분자 내 원자 정보 | 12,333 | 3 | `atom_id`, `molecule_id`, `element` | atom_id=TR000_1; molecule_id=TR000; element=cl |
| `bond` | 원자 간 결합 정보 | 12,379 | 3 | `bond_id`, `molecule_id`, `bond_type` | bond_id=TR000_1_2; molecule_id=TR000; bond_type=- |
| `connected` | 분자 연결 관계 | 24,758 | 3 | `atom_id`, `atom_id2`, `bond_id` | atom_id=TR000_1; atom_id2=TR000_2; bond_id=TR000_1_2 |
| `molecule` | 분자와 독성 라벨 | 343 | 2 | `molecule_id`, `label` | molecule_id=TR000; label=+ |

## student_club

학생 동아리 회원, 행사, 예산, 수입, 지출 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `event` | 동아리 행사 | 42 | 7 | `event_id`, `event_name`, `event_date`, `type`, `notes`, `location`, `status` | event_id=rec0Si5cQ4rJRVzd6; event_name=March Meeting; event_date=2020-03-10T12:00:00; type=Meeting |
| `major` | 전공 코드 | 113 | 4 | `major_id`, `major_name`, `department`, `college` | major_id=rec06DF6vZ1CyPKpc; major_name=Outdoor Product Design and Development; department=School of Applied Sciences, Technology and Ed...; college=College of Agriculture and Applied Sciences |
| `zip_code` | 우편번호와 위치 정보 | 41,877 | 6 | `zip_code`, `type`, `city`, `county`, `state`, `short_state` | zip_code=501; type=Unique; city=Holtsville; county=Suffolk County |
| `attendance` | 행사 참석 기록 | 326 | 2 | `link_to_event`, `link_to_member` | link_to_event=rec2N69DMcrqN9PJC; link_to_member=recD078PnS3x2doBe |
| `budget` | 동아리 예산 | 52 | 7 | `budget_id`, `link_to_event`, `category`, `spent`, `remaining`, `amount`, `event_status` | budget_id=rec0QmEc3cSQFQ6V2; link_to_event=recI43CzsZ0Q625ma; category=Advertisement; spent=67.81 |
| `expense` | 지출 내역 | 32 | 7 | `expense_id`, `link_to_member`, `link_to_budget`, `expense_description`, `expense_date`, `cost`, `approved` | expense_id=rec017x6R3hQqkLAo; link_to_member=rec4BLdZHS2Blfp4v; link_to_budget=recvKTAWAFKkVNnXQ; expense_description=Post Cards, Posters |
| `income` | 수입 내역 | 36 | 6 | `income_id`, `link_to_member`, `date_received`, `amount`, `source`, `notes` | income_id=rec0s9ZrO15zhzUeE; link_to_member=reccW7q1KkhSKZsea; date_received=2019-10-17; amount=50 |
| `member` | 회원 정보 | 33 | 9 | `member_id`, `zip`, `link_to_major`, `first_name`, `last_name`, `email`, `position` | member_id=rec1x5zBFIqoOuPW8; zip=55108; link_to_major=recxK3MHQFbR9J5uO; first_name=Angela |

## superhero

슈퍼히어로 인물, 능력, 속성, 출판사, 정렬 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `alignment` | 히어로 성향 | 4 | 2 | `id`, `alignment` | id=1; alignment=Good |
| `attribute` | 속성 종류 | 6 | 2 | `id`, `attribute_name` | id=1; attribute_name=Intelligence |
| `colour` | 색상 코드 | 35 | 2 | `id`, `colour` | id=1; colour=No Colour |
| `gender` | 성별 코드 | 3 | 2 | `id`, `gender` | id=1; gender=Male |
| `publisher` | 출판사 | 25 | 2 | `id`, `publisher_name` | id=1; publisher_name= |
| `race` | 종족 | 61 | 2 | `id`, `race` | id=1; race=- |
| `superhero` | 슈퍼히어로 기본 정보 | 750 | 12 | `id`, `gender_id`, `eye_colour_id`, `hair_colour_id`, `skin_colour_id`, `race_id`, `publisher_id` | id=1; gender_id=1; eye_colour_id=9; hair_colour_id=13 |
| `hero_attribute` | 히어로별 속성 값 | 3,738 | 3 | `hero_id`, `attribute_id`, `attribute_value` | hero_id=1; attribute_id=1; attribute_value=80 |
| `superpower` | 능력 종류 | 167 | 2 | `id`, `power_name` | id=1; power_name=Agility |
| `hero_power` | 히어로별 능력 매핑 | 5,825 | 2 | `hero_id`, `power_id` | hero_id=1; power_id=1 |

## codebase_community

개발자 커뮤니티 게시글, 댓글, 투표, 태그, 사용자 데이터

| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |
|---|---|---:|---:|---|---|
| `badges` | 사용자 배지 | 79,851 | 4 | `Id`, `UserId`, `Name`, `Date` | Id=1; UserId=5; Name=Teacher; Date=2010-07-19 19:39:07.0 |
| `comments` | 게시글 댓글 | 174,285 | 7 | `Id`, `PostId`, `UserId`, `Score`, `Text`, `CreationDate`, `UserDisplayName` | Id=1; PostId=3; UserId=13; Score=5 |
| `postHistory` | 게시글 변경 이력 | 303,155 | 9 | `Id`, `PostId`, `UserId`, `PostHistoryTypeId`, `RevisionGUID`, `CreationDate`, `Text` | Id=1; PostId=1; UserId=8; PostHistoryTypeId=2 |
| `postLinks` | 게시글 간 링크 | 11,102 | 5 | `Id`, `PostId`, `RelatedPostId`, `CreationDate`, `LinkTypeId` | Id=108; PostId=395; RelatedPostId=173; CreationDate=2010-07-21 14:47:33.0 |
| `posts` | 질문/답변 게시글 | 91,966 | 21 | `Id`, `OwnerUserId`, `LastEditorUserId`, `ParentId`, `PostTypeId`, `AcceptedAnswerId`, `CreaionDate` | Id=1; OwnerUserId=8; LastEditorUserId=NULL; ParentId=NULL |
| `tags` | 태그 통계 | 1,032 | 5 | `Id`, `ExcerptPostId`, `TagName`, `Count`, `WikiPostId` | Id=1; ExcerptPostId=20258; TagName=bayesian; Count=1342 |
| `users` | 커뮤니티 사용자 | 40,325 | 14 | `Id`, `Reputation`, `CreationDate`, `DisplayName`, `LastAccessDate`, `WebsiteUrl`, `Location` | Id=-1; Reputation=1; CreationDate=2010-07-19 06:55:26.0; DisplayName=Community |
| `votes` | 게시글 투표 | 38,930 | 6 | `Id`, `PostId`, `UserId`, `VoteTypeId`, `CreationDate`, `BountyAmount` | Id=1; PostId=3; UserId=NULL; VoteTypeId=2 |
