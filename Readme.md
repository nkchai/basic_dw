---
jupyter:
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
  language_info:
    codemirror_mode:
      name: ipython
      version: 3
    file_extension: .py
    mimetype: text/x-python
    name: python
    nbconvert_exporter: python
    pygments_lexer: ipython3
    version: 3.9.13
  nbformat: 4
  nbformat_minor: 2
---

<div class="cell markdown">

# DATA WAREHOUSING BASIC DW USING DUCKDB

</div>

<div class="cell markdown">

### Connecting to the Database

</div>

<div class="cell code" execution_count="12">

``` python
import duckdb 
duck_con = duckdb.connect('basic_dw.db')
```

</div>

<div class="cell markdown">

### Reading Parquet files to the tables

</div>

<div class="cell markdown">

The duckdb can automatically read the parquet files and create tables
with the schema inferred from the parquet file metedata.

</div>

<div class="cell code" execution_count="13">

``` python

duck_con.sql("create or replace table customer_table as select * from 'https://azuresynapsestorage.blob.core.windows.net/sampledata/WideWorldImportersDW/parquet/tables/DimCustomer.parquet'")

duck_con.sql("create or replace table city_table as select * from 'https://azuresynapsestorage.blob.core.windows.net/sampledata/WideWorldImportersDW/tables/dimension_city.parquet'")

duck_con.sql("create or replace table sales_table as select * from 'https://azuresynapsestorage.blob.core.windows.net/sampledata/WideWorldImportersDW/tables/fact_sale.parquet'")


```

</div>

<div class="cell markdown">

### Description(schema) of all 3 Tables

</div>

<div class="cell code" execution_count="15">

``` python
duck_con.sql("describe select * from customer_table")
```

<div class="output execute_result" execution_count="15">

    ┌────────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┐
    │  column_name   │ column_type │  null   │   key   │ default │  extra  │
    │    varchar     │   varchar   │ varchar │ varchar │ varchar │ varchar │
    ├────────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┤
    │ CustomerKey    │ INTEGER     │ YES     │ NULL    │ NULL    │ NULL    │
    │ WWICustomerID  │ INTEGER     │ YES     │ NULL    │ NULL    │ NULL    │
    │ Customer       │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ BillToCustomer │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ Category       │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ BuyingGroup    │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ PrimaryContact │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ PostalCode     │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ ValidFrom      │ TIMESTAMP   │ YES     │ NULL    │ NULL    │ NULL    │
    │ ValidTo        │ TIMESTAMP   │ YES     │ NULL    │ NULL    │ NULL    │
    │ LineageKey     │ INTEGER     │ YES     │ NULL    │ NULL    │ NULL    │
    ├────────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┤
    │ 11 rows                                                    6 columns │
    └──────────────────────────────────────────────────────────────────────┘

</div>

</div>

<div class="cell code" execution_count="16">

``` python
duck_con.sql("describe select * from city_table")
```

<div class="output execute_result" execution_count="16">

    ┌──────────────────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┐
    │       column_name        │ column_type │  null   │   key   │ default │  extra  │
    │         varchar          │   varchar   │ varchar │ varchar │ varchar │ varchar │
    ├──────────────────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┤
    │ CityKey                  │ INTEGER     │ YES     │ NULL    │ NULL    │ NULL    │
    │ WWICityID                │ INTEGER     │ YES     │ NULL    │ NULL    │ NULL    │
    │ City                     │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ StateProvince            │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ Country                  │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ Continent                │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ SalesTerritory           │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ Region                   │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ Subregion                │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ Location                 │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    │ LatestRecordedPopulation │ BIGINT      │ YES     │ NULL    │ NULL    │ NULL    │
    │ ValidFrom                │ TIMESTAMP   │ YES     │ NULL    │ NULL    │ NULL    │
    │ ValidTo                  │ TIMESTAMP   │ YES     │ NULL    │ NULL    │ NULL    │
    │ LineageKey               │ INTEGER     │ YES     │ NULL    │ NULL    │ NULL    │
    ├──────────────────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┤
    │ 14 rows                                                              6 columns │
    └────────────────────────────────────────────────────────────────────────────────┘

</div>

</div>

<div class="cell code" execution_count="17">

``` python
duck_con.sql("describe select * from sales_table")
```

<div class="output execute_result" execution_count="17">

    ┌───────────────────┬───────────────┬─────────┬─────────┬─────────┬─────────┐
    │    column_name    │  column_type  │  null   │   key   │ default │  extra  │
    │      varchar      │    varchar    │ varchar │ varchar │ varchar │ varchar │
    ├───────────────────┼───────────────┼─────────┼─────────┼─────────┼─────────┤
    │ SaleKey           │ BIGINT        │ YES     │ NULL    │ NULL    │ NULL    │
    │ CityKey           │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    │ CustomerKey       │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    │ BillToCustomerKey │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    │ StockItemKey      │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    │ InvoiceDateKey    │ TIMESTAMP     │ YES     │ NULL    │ NULL    │ NULL    │
    │ DeliveryDateKey   │ TIMESTAMP     │ YES     │ NULL    │ NULL    │ NULL    │
    │ SalespersonKey    │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    │ WWIInvoiceID      │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    │ Description       │ VARCHAR       │ YES     │ NULL    │ NULL    │ NULL    │
    │ Package           │ VARCHAR       │ YES     │ NULL    │ NULL    │ NULL    │
    │ Quantity          │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    │ UnitPrice         │ DECIMAL(18,2) │ YES     │ NULL    │ NULL    │ NULL    │
    │ TaxRate           │ DECIMAL(18,3) │ YES     │ NULL    │ NULL    │ NULL    │
    │ TotalExcludingTax │ DECIMAL(18,2) │ YES     │ NULL    │ NULL    │ NULL    │
    │ TaxAmount         │ DECIMAL(18,2) │ YES     │ NULL    │ NULL    │ NULL    │
    │ Profit            │ DECIMAL(18,2) │ YES     │ NULL    │ NULL    │ NULL    │
    │ TotalIncludingTax │ DECIMAL(18,2) │ YES     │ NULL    │ NULL    │ NULL    │
    │ TotalDryItems     │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    │ TotalChillerItems │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    │ LineageKey        │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    ├───────────────────┴───────────────┴─────────┴─────────┴─────────┴─────────┤
    │ 21 rows                                                         6 columns │
    └───────────────────────────────────────────────────────────────────────────┘

</div>

</div>

<div class="cell markdown">

### Creating a Date table with min and max dates

</div>

<div class="cell code" execution_count="18">

``` python
# import datetime
# min_date = (duckdb.sql("select min(InvoiceDateKey) from sales_table").fetchone()[0])
# print(min_date)
# #print(min_date.strftime("%y-%m-%d"))
# max_date = (duckdb.sql("select max(InvoiceDateKey) from sales_table").fetchone()[0])
# print(max_date)

date_dim = duck_con.sql(''' create or replace table date_dim as 
                      with days as (
                      select 
                        year("generate_series") as year,
                        quarter("generate_series") as quarter,
                        month("generate_series") as month,
                        week("generate_series") as week,
                        dayofweek("generate_series") as dow,
                        "generate_series" as date
                    from generate_series(date '2000-01-01' ,date '2000-11-30' , interval 1 day)
                    )
                    select year,quarter,month,week,dow,date 
                    from days
                    order by date 
                       ''')

duck_con.sql("select * from date_dim")
```

<div class="output execute_result" execution_count="18">

    ┌───────┬─────────┬───────┬───────┬───────┬─────────────────────┐
    │ year  │ quarter │ month │ week  │  dow  │        date         │
    │ int64 │  int64  │ int64 │ int64 │ int64 │      timestamp      │
    ├───────┼─────────┼───────┼───────┼───────┼─────────────────────┤
    │  2000 │       1 │     1 │    52 │     6 │ 2000-01-01 00:00:00 │
    │  2000 │       1 │     1 │    52 │     0 │ 2000-01-02 00:00:00 │
    │  2000 │       1 │     1 │     1 │     1 │ 2000-01-03 00:00:00 │
    │  2000 │       1 │     1 │     1 │     2 │ 2000-01-04 00:00:00 │
    │  2000 │       1 │     1 │     1 │     3 │ 2000-01-05 00:00:00 │
    │  2000 │       1 │     1 │     1 │     4 │ 2000-01-06 00:00:00 │
    │  2000 │       1 │     1 │     1 │     5 │ 2000-01-07 00:00:00 │
    │  2000 │       1 │     1 │     1 │     6 │ 2000-01-08 00:00:00 │
    │  2000 │       1 │     1 │     1 │     0 │ 2000-01-09 00:00:00 │
    │  2000 │       1 │     1 │     2 │     1 │ 2000-01-10 00:00:00 │
    │    ·  │       · │     · │     · │     · │          ·          │
    │    ·  │       · │     · │     · │     · │          ·          │
    │    ·  │       · │     · │     · │     · │          ·          │
    │  2000 │       4 │    11 │    47 │     2 │ 2000-11-21 00:00:00 │
    │  2000 │       4 │    11 │    47 │     3 │ 2000-11-22 00:00:00 │
    │  2000 │       4 │    11 │    47 │     4 │ 2000-11-23 00:00:00 │
    │  2000 │       4 │    11 │    47 │     5 │ 2000-11-24 00:00:00 │
    │  2000 │       4 │    11 │    47 │     6 │ 2000-11-25 00:00:00 │
    │  2000 │       4 │    11 │    47 │     0 │ 2000-11-26 00:00:00 │
    │  2000 │       4 │    11 │    48 │     1 │ 2000-11-27 00:00:00 │
    │  2000 │       4 │    11 │    48 │     2 │ 2000-11-28 00:00:00 │
    │  2000 │       4 │    11 │    48 │     3 │ 2000-11-29 00:00:00 │
    │  2000 │       4 │    11 │    48 │     4 │ 2000-11-30 00:00:00 │
    ├───────┴─────────┴───────┴───────┴───────┴─────────────────────┤
    │ 335 rows (20 shown)                                 6 columns │
    └───────────────────────────────────────────────────────────────┘

</div>

</div>

<div class="cell markdown">

### Creating a Aggregated sales fact table

</div>

<div class="cell code" execution_count="19">

``` python
#Create an aggregate fact table using Sales Table with the following columns,InvoiceDateKey] AS [Date],  City, 
#StateProvince, SalesTerritory, TotalExcludingTax, TaxAmount,TotalIncludingTax and Profit 
#and grouped by Invoice Date, City, State and Sales Territory
#and Ordered by Invoice Date, StateProvince and City

agg_fact = duck_con.sql(''' select s.CustomerKey, InvoiceDateKey as Date , City , StateProvince, SalesTerritory, 
                        TotalExcludingTax, TaxAmount, TotalIncludingTax, Profit from sales_table s
                        inner join city_table on s.CityKey = city_table.CityKey
                        inner join customer_table on s.CustomerKey = customer_table.CustomerKey
                     ''' )

duck_con.sql("create or replace table agg_sales_fact_table as select * from agg_fact")

```

</div>

<div class="cell markdown">

### Description(schema) of aggregated fact table

</div>

<div class="cell code" execution_count="20">

``` python
duck_con.sql("describe select * from agg_sales_fact_table")
```

<div class="output execute_result" execution_count="20">

    ┌───────────────────┬───────────────┬─────────┬─────────┬─────────┬─────────┐
    │    column_name    │  column_type  │  null   │   key   │ default │  extra  │
    │      varchar      │    varchar    │ varchar │ varchar │ varchar │ varchar │
    ├───────────────────┼───────────────┼─────────┼─────────┼─────────┼─────────┤
    │ CustomerKey       │ INTEGER       │ YES     │ NULL    │ NULL    │ NULL    │
    │ Date              │ TIMESTAMP     │ YES     │ NULL    │ NULL    │ NULL    │
    │ City              │ VARCHAR       │ YES     │ NULL    │ NULL    │ NULL    │
    │ StateProvince     │ VARCHAR       │ YES     │ NULL    │ NULL    │ NULL    │
    │ SalesTerritory    │ VARCHAR       │ YES     │ NULL    │ NULL    │ NULL    │
    │ TotalExcludingTax │ DECIMAL(18,2) │ YES     │ NULL    │ NULL    │ NULL    │
    │ TaxAmount         │ DECIMAL(18,2) │ YES     │ NULL    │ NULL    │ NULL    │
    │ TotalIncludingTax │ DECIMAL(18,2) │ YES     │ NULL    │ NULL    │ NULL    │
    │ Profit            │ DECIMAL(18,2) │ YES     │ NULL    │ NULL    │ NULL    │
    └───────────────────┴───────────────┴─────────┴─────────┴─────────┴─────────┘

</div>

</div>

<div class="cell markdown">

### Profit for a Specific Cusotmer

</div>

<div class="cell code" execution_count="21">

``` python
# Profit for a specific customer,

duck_con.sql('''select 
                a.CustomerKey, 
                b.Customer,
                sum(a.Profit) as TotalProfit
            from 
                agg_sales_fact_table a
            join 
                customer_table b ON a.CustomerKey = b.CustomerKey 
            Where a.CustomerKey = 387
            group by a.CustomerKey, b.Customer''')


```

<div class="output execute_result" execution_count="21">

    ┌─────────────┬──────────────────────────┬───────────────┐
    │ CustomerKey │         Customer         │  TotalProfit  │
    │    int32    │         varchar          │ decimal(38,2) │
    ├─────────────┼──────────────────────────┼───────────────┤
    │         387 │ Wingtip Toys (Lynne, FL) │   95423040.00 │
    └─────────────┴──────────────────────────┴───────────────┘

</div>

</div>

<div class="cell markdown">

### Number of the Years of data the dataset contains

</div>

<div class="cell code" execution_count="23">

``` python
duck_con.sql(''' select distinct year(InvoiceDateKey) from sales_table''')
```

<div class="output execute_result" execution_count="23">

    ┌──────────────────────┐
    │ year(InvoiceDateKey) │
    │        int64         │
    ├──────────────────────┤
    │                 2000 │
    └──────────────────────┘

</div>

</div>

<div class="cell markdown">

### List of customers in a city and sales for a specific year

</div>

<div class="cell markdown">

Note: The dataset contains the data only for year 2000 and I have
created two queries one lists all the customer in a city(Sinclair) and
their respective sales for year 2000 and another displays all the city
names how many customer's each cit and total sales of all customers in
that city.

</div>

<div class="cell code" execution_count="24">

``` python
# List of customers in a city and sales for a specific year


duck_con.sql(''' select CustomerKey, sum(TotalIncludingTax) as Year2000Sales
             from agg_sales_fact_table 
             where City = 'Sinclair' and year(DATE) = 2000
             group by  CustomerKey ''')


```

<div class="output execute_result" execution_count="24">

    ┌─────────────┬───────────────┐
    │ CustomerKey │ Year2000Sales │
    │    int32    │ decimal(38,2) │
    ├─────────────┼───────────────┤
    │           0 │  100283456.00 │
    │         169 │  129991363.20 │
    └─────────────┴───────────────┘

</div>

</div>

<div class="cell code" execution_count="22">

``` python
duck_con.sql(''' select distinct(City),count(distinct(CustomerKey)) as customers,sum(TotalIncludingTax) as Year2000Sale
              from agg_sales_fact_table 
              group by City 
              order by customers desc ''')
```

<div class="output execute_result" execution_count="22">

    ┌─────────────────┬───────────┬───────────────┐
    │      City       │ customers │ Year2000Sale  │
    │     varchar     │   int64   │ decimal(38,2) │
    ├─────────────────┼───────────┼───────────────┤
    │ East Fultonham  │         2 │   45126552.00 │
    │ Akhiok          │         2 │  231603670.40 │
    │ Teutopolis      │         2 │  435594470.00 │
    │ Sinclair        │         2 │  230274819.20 │
    │ Rockwall        │         2 │  238622456.40 │
    │ Corfu           │         1 │  165754998.40 │
    │ Railroad        │         1 │   59889424.00 │
    │ East Mountain   │         1 │   30016876.80 │
    │ Humansville     │         1 │   96700832.00 │
    │ Trumansburg     │         1 │   51709606.50 │
    │   ·             │         · │        ·      │
    │   ·             │         · │        ·      │
    │   ·             │         · │        ·      │
    │ Buell           │         1 │   70072352.00 │
    │ Deer River      │         1 │  125252480.00 │
    │ Hayes Center    │         1 │   31658304.00 │
    │ Spillertown     │         1 │   49040784.00 │
    │ Miesville       │         1 │  152832681.60 │
    │ La Paz          │         1 │  162794989.00 │
    │ Branchburg Park │         1 │   72951001.65 │
    │ Bernie          │         1 │  238399968.00 │
    │ Fortville       │         1 │   92526688.96 │
    │ Oley            │         1 │   10552768.00 │
    ├─────────────────┴───────────┴───────────────┤
    │ 463 rows (20 shown)               3 columns │
    └─────────────────────────────────────────────┘

</div>

</div>

<div class="cell markdown">

### Using date and city and the aggregate sales fact table result of Total incluing tax and profit by region duing a specific quarter

</div>

<div class="cell markdown">

Note: Instead of a specific quarter I chose to give the data for all
four quarters

</div>

<div class="cell code" execution_count="25">

``` python
# Using date and city and the aggregate sales fact table result of Total incluing tax and profit by region duing a specific quarter

duck_con.sql('''select b.region , quarter , sum(a.TotalIncludingTax) as TotalSales , sum(a.profit) as Totalprofit
                from agg_sales_fact_table a 
                join 
                date_dim d on a.Date = d.Date
                join 
                city_table b on a.City = b.City
                group by  region , quarter
                order by quarter''' )

```

<div class="output execute_result" execution_count="25">

    ┌──────────┬─────────┬────────────────┬────────────────┐
    │  Region  │ quarter │   TotalSales   │  Totalprofit   │
    │ varchar  │  int64  │ decimal(38,2)  │ decimal(38,2)  │
    ├──────────┼─────────┼────────────────┼────────────────┤
    │ Americas │       1 │ 35693201665.06 │ 15445288592.80 │
    │ Americas │       2 │ 35738582209.44 │ 15466226908.10 │
    │ Americas │       3 │ 36045198650.65 │ 15598495212.60 │
    │ Americas │       4 │ 23893331813.38 │ 10339890223.00 │
    └──────────┴─────────┴────────────────┴────────────────┘

</div>

</div>

<div class="cell markdown">

### Result of Tax amount by territory during a quarter

</div>

<div class="cell code" execution_count="26">

``` python
# result of Tax amount by territory during a quarter

duck_con.sql(''' select SalesTerritory  , sum(a.TaxAmount) as TaxAmount
               from agg_sales_fact_table a
               join 
               date_dim d on a.Date = d.Date
               where quarter = 1
               group by SalesTerritory ''')
```

<div class="output execute_result" execution_count="26">

    ┌────────────────┬───────────────┐
    │ SalesTerritory │   TaxAmount   │
    │    varchar     │ decimal(38,2) │
    ├────────────────┼───────────────┤
    │ Great Lakes    │  196404472.76 │
    │ Far West       │  127682140.04 │
    │ Mideast        │  249599009.90 │
    │ Rocky Mountain │   74137402.43 │
    │ Southeast      │  352894028.04 │
    │ External       │   10543832.80 │
    │ Southwest      │  129178961.81 │
    │ New England    │  101234636.54 │
    │ Plains         │  233113005.73 │
    └────────────────┴───────────────┘

</div>

</div>

<div class="cell markdown">

### List of Tables present in the basic_dw.db

</div>

<div class="cell code" execution_count="27">

``` python
duck_con.sql("show tables")
```

<div class="output execute_result" execution_count="27">

    ┌──────────────────────┐
    │         name         │
    │       varchar        │
    ├──────────────────────┤
    │ agg_sales_fact_table │
    │ city_table           │
    │ customer_table       │
    │ date_dim             │
    │ sales_table          │
    └──────────────────────┘

</div>

</div>
