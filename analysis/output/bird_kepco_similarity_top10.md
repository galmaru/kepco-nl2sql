# BIRD DB Similarity Top 10 for KEPCO NL2SQL

| Rank | Split.DB | KEPCO 유사 포인트 | Tables | Columns | Rows | Questions |
|---:|---|---|---:|---:|---:|---:|
| 1 | `train.sales_in_weather` | 전력 사용량/계량값 + 기상/지점 조인 패턴 | 3 | 26 | 4,638,162 | 80 |
| 2 | `dev.debit_card_specializing` | 고객별 월별 사용량, 거래 금액, 단가 패턴 | 5 | 21 | 423,050 | 64 |
| 3 | `train.regional_sales` | 지역/고객/상품/주문/매출 집계 패턴 | 6 | 41 | 8,531 | 164 |
| 4 | `train.superstore` | 지역별 고객·상품·판매 데이터 마트 패턴 | 6 | 61 | 27,787 | 116 |
| 5 | `train.retails` | 대규모 고객·주문·청구/라인아이템 조인 패턴 | 8 | 61 | 7,083,689 | 245 |
| 6 | `train.bike_share_1` | 설비/스테이션 상태 이력과 시간대 운영 패턴 | 4 | 46 | 72,647,070 | 113 |
| 7 | `train.works_cycles` | 실무 ERP형 복잡 조인, 주문·생산·재고·거래 이력 | 65 | 455 | 719,435 | 474 |
| 8 | `dev.financial` | 고객 계좌, 거래, 대출, 납부 금액 패턴 | 8 | 55 | 1,079,680 | 106 |
| 9 | `train.car_retails` | 고객, 주문, 결제, 상품, 사업소/직원 패턴 | 8 | 59 | 3,864 | 126 |
| 10 | `train.synthea` | 고객/환자 단위 이벤트, 관측값, 청구/처치 이력 패턴 | 11 | 85 | 170,810 | 185 |

## 1. train.sales_in_weather

- KEPCO 유사 포인트: 전력 사용량/계량값 + 기상/지점 조인 패턴
- 규모: 3 tables, 26 columns, 4,638,162 rows, 80 questions

| Table | Rows | 주요 컬럼 | 샘플 값 |
|---|---:|---|---|
| `sales_in_weather` | 4,617,600 | `date`, `store_nbr`, `item_nbr`, `units` | date=2012-01-01; store_nbr=1; item_nbr=1; units=0 |
| `weather` | 20,517 | `station_nbr`, `date`, `tmax`, `tmin`, `tavg`, `depart` | station_nbr=1; date=2012-01-01; tmax=52; tmin=31 |
| `relation` | 45 | `store_nbr`, `station_nbr` | store_nbr=1; station_nbr=1 |

## 2. dev.debit_card_specializing

- KEPCO 유사 포인트: 고객별 월별 사용량, 거래 금액, 단가 패턴
- 규모: 5 tables, 21 columns, 423,050 rows, 64 questions

| Table | Rows | 주요 컬럼 | 샘플 값 |
|---|---:|---|---|
| `yearmonth` | 383,282 | `CustomerID`, `Date`, `Consumption` | CustomerID=5; Date=201207; Consumption=528.3 |
| `customers` | 32,461 | `CustomerID`, `Segment`, `Currency` | CustomerID=3; Segment=SME; Currency=EUR |
| `gasstations` | 5,716 | `GasStationID`, `ChainID`, `Country`, `Segment` | GasStationID=44; ChainID=13; Country=CZE; Segment=Value for money |
| `transactions_1k` | 1,000 | `TransactionID`, `Date`, `Time`, `CustomerID`, `CardID`, `GasStationID` | TransactionID=1; Date=2012-08-24; Time=09:41:00; CustomerID=31543 |
| `products` | 591 | `ProductID`, `Description` | ProductID=1; Description=Rucní zadání |

## 3. train.regional_sales

- KEPCO 유사 포인트: 지역/고객/상품/주문/매출 집계 패턴
- 규모: 6 tables, 41 columns, 8,531 rows, 164 questions

| Table | Rows | 주요 컬럼 | 샘플 값 |
|---|---:|---|---|
| `Sales Orders` | 7,991 | `OrderNumber`, `_SalesTeamID`, `_CustomerID`, `_StoreID`, `_ProductID`, `Sales Channel` | OrderNumber=SO - 000101; _SalesTeamID=6; _CustomerID=15; _StoreID=259 |
| `Store Locations` | 367 | `StoreID`, `StateCode`, `City Name`, `County`, `State`, `Type` | StoreID=1; StateCode=AL; City Name=Birmingham; County=Shelby County/Jefferson County |
| `Customers` | 50 | `CustomerID`, `Customer Names` | CustomerID=1; Customer Names=Avon Corp |
| `Regions` | 48 | `StateCode`, `State`, `Region` | StateCode=AL; State=Alabama; Region=South |
| `Products` | 47 | `ProductID`, `Product Name` | ProductID=1; Product Name=Cookware |
| `Sales Team` | 28 | `SalesTeamID`, `Sales Team`, `Region` | SalesTeamID=1; Sales Team=Adam Hernandez; Region=Northeast |

## 4. train.superstore

- KEPCO 유사 포인트: 지역별 고객·상품·판매 데이터 마트 패턴
- 규모: 6 tables, 61 columns, 27,787 rows, 116 questions

| Table | Rows | 주요 컬럼 | 샘플 값 |
|---|---:|---|---|
| `west_superstore` | 6,406 | `Row ID`, `Customer ID`, `Region`, `Product ID`, `Order ID`, `Order Date` | Row ID=13583; Customer ID=LS-17230; Region=West; Product ID=OFF-PA-10002005 |
| `east_superstore` | 5,696 | `Row ID`, `Customer ID`, `Region`, `Product ID`, `Order ID`, `Order Date` | Row ID=4647; Customer ID=MB-18085; Region=East; Product ID=OFF-AR-10003478 |
| `product` | 5,298 | `Product ID`, `Region`, `Product Name`, `Category`, `Sub-Category` | Product ID=FUR-BO-10000330; Region=West; Product Name=Sauder Camden County Barrister Bookcase, Planked Cherry Finish; Category=Furniture |
| `central_superstore` | 4,646 | `Row ID`, `Customer ID`, `Region`, `Product ID`, `Order ID`, `Order Date` | Row ID=1; Customer ID=DP-13000; Region=Central; Product ID=OFF-PA-10000174 |
| `south_superstore` | 3,240 | `Row ID`, `Customer ID`, `Region`, `Product ID`, `Order ID`, `Order Date` | Row ID=10343; Customer ID=JO-15145; Region=South; Product ID=OFF-AR-10002399 |
| `people` | 2,501 | `Customer ID`, `Region`, `Customer Name`, `Segment`, `Country`, `City` | Customer ID=AA-10315; Region=Central; Customer Name=Alex Avila; Segment=Consumer |

## 5. train.retails

- KEPCO 유사 포인트: 대규모 고객·주문·청구/라인아이템 조인 패턴
- 규모: 8 tables, 61 columns, 7,083,689 rows, 245 questions

| Table | Rows | 주요 컬럼 | 샘플 값 |
|---|---:|---|---|
| `lineitem` | 4,423,659 | `l_orderkey`, `l_linenumber`, `l_suppkey`, `l_partkey`, `l_shipdate`, `l_discount` | l_orderkey=1; l_linenumber=1; l_suppkey=6296; l_partkey=98768 |
| `orders` | 1,500,000 | `o_orderkey`, `o_custkey`, `o_orderdate`, `o_orderpriority`, `o_shippriority`, `o_clerk` | o_orderkey=1; o_custkey=73100; o_orderdate=1995-04-19; o_orderpriority=4-NOT SPECIFIED |
| `partsupp` | 800,000 | `ps_partkey`, `ps_suppkey`, `ps_supplycost`, `ps_availqty`, `ps_comment` | ps_partkey=1; ps_suppkey=2; ps_supplycost=400.75; ps_availqty=1111 |
| `part` | 200,000 | `p_partkey`, `p_type`, `p_size`, `p_brand`, `p_name`, `p_container` | p_partkey=1; p_type=LARGE PLATED TIN; p_size=31; p_brand=Brand#43 |
| `customer` | 150,000 | `c_custkey`, `c_nationkey`, `c_mktsegment`, `c_name`, `c_address`, `c_phone` | c_custkey=1; c_nationkey=8; c_mktsegment=BUILDING; c_name=Customer#000000001 |
| `supplier` | 10,000 | `s_suppkey`, `s_nationkey`, `s_comment`, `s_name`, `s_address`, `s_phone` | s_suppkey=1; s_nationkey=13; s_comment=blithely final pearls are. instructions thra; s_name=Supplier#000000001 |
| `nation` | 25 | `n_nationkey`, `n_regionkey`, `n_name`, `n_comment` | n_nationkey=0; n_regionkey=0; n_name=ALGERIA; n_comment=slyly express pinto beans cajole idly. deposits use blithely unusua... |
| `region` | 5 | `r_regionkey`, `r_name`, `r_comment` | r_regionkey=0; r_name=AFRICA; r_comment=asymptotes sublate after the r |

## 6. train.bike_share_1

- KEPCO 유사 포인트: 설비/스테이션 상태 이력과 시간대 운영 패턴
- 규모: 4 tables, 46 columns, 72,647,070 rows, 113 questions

| Table | Rows | 주요 컬럼 | 샘플 값 |
|---|---:|---|---|
| `status` | 71,984,434 | `station_id`, `bikes_available`, `docks_available`, `time` | station_id=2; bikes_available=2; docks_available=25; time=2013/08/29 12:06:01 |
| `trip` | 658,901 | `id`, `duration`, `start_date`, `start_station_name`, `start_station_id`, `end_date` | id=4069; duration=174; start_date=8/29/2013 9:08; start_station_name=2nd at South Park |
| `weather` | 3,665 | `date`, `max_temperature_f`, `mean_temperature_f`, `min_temperature_f`, `max_dew_point_f`, `mean_dew_point_f` | date=8/29/2013; max_temperature_f=74; mean_temperature_f=68; min_temperature_f=61 |
| `station` | 70 | `id`, `name`, `lat`, `long`, `dock_count`, `city` | id=2; name=San Jose Diridon Caltrain Station; lat=37.329732; long=-121.90178200000001 |

## 7. train.works_cycles

- KEPCO 유사 포인트: 실무 ERP형 복잡 조인, 주문·생산·재고·거래 이력
- 규모: 65 tables, 455 columns, 719,435 rows, 474 questions

| Table | Rows | 주요 컬럼 | 샘플 값 |
|---|---:|---|---|
| `SalesOrderDetail` | 121,317 | `SalesOrderDetailID`, `SalesOrderID`, `ProductID`, `SpecialOfferID`, `CarrierTrackingNumber`, `OrderQty` | SalesOrderDetailID=1; SalesOrderID=43659; ProductID=776; SpecialOfferID=1 |
| `TransactionHistory` | 113,443 | `TransactionID`, `ProductID`, `ReferenceOrderID`, `ReferenceOrderLineID`, `TransactionDate`, `TransactionType` | TransactionID=100000; ProductID=784; ReferenceOrderID=41590; ReferenceOrderLineID=0 |
| `TransactionHistoryArchive` | 89,253 | `TransactionID`, `ProductID`, `ReferenceOrderID`, `ReferenceOrderLineID`, `TransactionDate`, `TransactionType` | TransactionID=1; ProductID=1; ReferenceOrderID=1; ReferenceOrderLineID=1 |
| `WorkOrder` | 72,591 | `WorkOrderID`, `ProductID`, `ScrapReasonID`, `OrderQty`, `StockedQty`, `ScrappedQty` | WorkOrderID=1; ProductID=722; ScrapReasonID=NULL; OrderQty=8 |
| `WorkOrderRouting` | 67,131 | `WorkOrderID`, `ProductID`, `OperationSequence`, `LocationID`, `ScheduledStartDate`, `ScheduledEndDate` | WorkOrderID=13; ProductID=747; OperationSequence=1; LocationID=10 |
| `SalesOrderHeader` | 31,465 | `SalesOrderID`, `CustomerID`, `SalesPersonID`, `TerritoryID`, `BillToAddressID`, `ShipToAddressID` | SalesOrderID=43659; CustomerID=29825; SalesPersonID=279; TerritoryID=5 |
| `SalesOrderHeaderSalesReason` | 27,647 | `SalesOrderID`, `SalesReasonID`, `ModifiedDate` | SalesOrderID=43697; SalesReasonID=5; ModifiedDate=2011-05-31 00:00:00.0 |
| `BusinessEntity` | 20,777 | `BusinessEntityID`, `rowguid`, `ModifiedDate` | BusinessEntityID=1; rowguid=0C7D8F81-D7B1-4CF0-9C0A-4CD8B6B50087; ModifiedDate=2017-12-13 13:20:24.0 |

## 8. dev.financial

- KEPCO 유사 포인트: 고객 계좌, 거래, 대출, 납부 금액 패턴
- 규모: 8 tables, 55 columns, 1,079,680 rows, 106 questions

| Table | Rows | 주요 컬럼 | 샘플 값 |
|---|---:|---|---|
| `trans` | 1,056,320 | `trans_id`, `account_id`, `date`, `type`, `operation`, `amount` | trans_id=1; account_id=1; date=1995-03-24; type=PRIJEM |
| `order` | 6,471 | `order_id`, `account_id`, `bank_to`, `account_to`, `amount`, `k_symbol` | order_id=29401; account_id=1; bank_to=YZ; account_to=87144583 |
| `client` | 5,369 | `client_id`, `district_id`, `gender`, `birth_date` | client_id=1; district_id=18; gender=F; birth_date=1970-12-13 |
| `disp` | 5,369 | `disp_id`, `client_id`, `account_id`, `type` | disp_id=1; client_id=1; account_id=1; type=OWNER |
| `account` | 4,500 | `account_id`, `district_id`, `frequency`, `date` | account_id=1; district_id=18; frequency=POPLATEK MESICNE; date=1995-03-24 |
| `card` | 892 | `card_id`, `disp_id`, `type`, `issued` | card_id=1; disp_id=9; type=gold; issued=1998-10-16 |
| `loan` | 682 | `loan_id`, `account_id`, `date`, `amount`, `duration`, `payments` | loan_id=4959; account_id=2; date=1994-01-05; amount=80952 |
| `district` | 77 | `district_id`, `A2`, `A3`, `A4`, `A5`, `A6` | district_id=1; A2=Hl.m. Praha; A3=Prague; A4=1204953 |

## 9. train.car_retails

- KEPCO 유사 포인트: 고객, 주문, 결제, 상품, 사업소/직원 패턴
- 규모: 8 tables, 59 columns, 3,864 rows, 126 questions

| Table | Rows | 주요 컬럼 | 샘플 값 |
|---|---:|---|---|
| `orderdetails` | 2,996 | `orderNumber`, `productCode`, `quantityOrdered`, `priceEach`, `orderLineNumber` | orderNumber=10100; productCode=S18_1749; quantityOrdered=30; priceEach=136.0 |
| `orders` | 326 | `orderNumber`, `customerNumber`, `orderDate`, `requiredDate`, `shippedDate`, `status` | orderNumber=10100; customerNumber=363; orderDate=2003-01-06; requiredDate=2003-01-13 |
| `payments` | 273 | `customerNumber`, `checkNumber`, `paymentDate`, `amount` | customerNumber=103; checkNumber=HQ336336; paymentDate=2004-10-19; amount=6066.78 |
| `customers` | 122 | `customerNumber`, `salesRepEmployeeNumber`, `customerName`, `contactLastName`, `contactFirstName`, `phone` | customerNumber=103; salesRepEmployeeNumber=1370; customerName=Atelier graphique; contactLastName=Schmitt |
| `products` | 110 | `productCode`, `productLine`, `productName`, `productScale`, `productVendor`, `productDescription` | productCode=S10_1678; productLine=Motorcycles; productName=1969 Harley Davidson Ultimate Chopper; productScale=1:10 |
| `employees` | 23 | `employeeNumber`, `officeCode`, `reportsTo`, `lastName`, `firstName`, `extension` | employeeNumber=1002; officeCode=1; reportsTo=NULL; lastName=Murphy |
| `offices` | 7 | `officeCode`, `city`, `phone`, `addressLine1`, `addressLine2`, `state` | officeCode=1; city=San Francisco; phone=+1 650 219 4782; addressLine1=100 Market Street |
| `productlines` | 7 | `productLine`, `textDescription`, `htmlDescription`, `image` | productLine=Classic Cars; textDescription=Attention car enthusiasts: Make your wildest car ownership dreams c...; htmlDescription=NULL; image=NULL |

## 10. train.synthea

- KEPCO 유사 포인트: 고객/환자 단위 이벤트, 관측값, 청구/처치 이력 패턴
- 규모: 11 tables, 85 columns, 170,810 rows, 185 questions

| Table | Rows | 주요 컬럼 | 샘플 값 |
|---|---:|---|---|
| `observations` | 78,899 | `PATIENT`, `ENCOUNTER`, `DATE`, `CODE`, `DESCRIPTION`, `VALUE` | PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=5114a5b4-64b8-47b2-82a6-0ce24aae0943; DATE=2008-03-11; CODE=8302-2 |
| `encounters` | 20,524 | `ID`, `PATIENT`, `DATE`, `CODE`, `DESCRIPTION`, `REASONCODE` | ID=5114a5b4-64b8-47b2-82a6-0ce24aae0943; PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; DATE=2008-03-11; CODE=185349003 |
| `claims` | 20,523 | `ID`, `PATIENT`, `ENCOUNTER`, `BILLABLEPERIOD`, `ORGANIZATION`, `DIAGNOSIS` | ID=1a9e880e-27a1-4465-8adc-222b1996a14a; PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=71949668-1c2e-43ae-ab0a-64654608defb; BILLABLEPERIOD=2008-03-11 |
| `immunizations` | 13,189 | `DATE`, `PATIENT`, `ENCOUNTER`, `CODE`, `DESCRIPTION` | DATE=2008-03-11; PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=5114a5b4-64b8-47b2-82a6-0ce24aae0943; CODE=140 |
| `careplans` | 12,125 | `PATIENT`, `ENCOUNTER`, `ID`, `START`, `STOP`, `CODE` | PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=4d451e22-a354-40c9-8b33-b6126158666d; ID=e031962d-d13d-4ede-a449-040417d5e4fb; START=2009-01-11 |
| `procedures` | 10,184 | `PATIENT`, `ENCOUNTER`, `DATE`, `CODE`, `DESCRIPTION`, `REASONCODE` | PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=6f2e3935-b203-493e-a9c0-f23e847b9798; DATE=2013-02-09; CODE=23426006 |
| `conditions` | 7,040 | `PATIENT`, `ENCOUNTER`, `DESCRIPTION`, `START`, `STOP`, `CODE` | PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=4d451e22-a354-40c9-8b33-b6126158666d; DESCRIPTION=Acute bronchitis (disorder); START=2009-01-08 |
| `medications` | 6,048 | `START`, `PATIENT`, `ENCOUNTER`, `CODE`, `STOP`, `DESCRIPTION` | START=1988-09-05; PATIENT=71949668-1c2e-43ae-ab0a-64654608defb; ENCOUNTER=5114a5b4-64b8-47b2-82a6-0ce24aae0943; CODE=834060 |
