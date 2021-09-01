## Tasks

[Initial tasks are here](initial_readme.md)

To do the following tasks, you probably gonna need some kinds data processing library in your own choice of programming language.
`pandas`, a data processing library in Python, is recommended due to ease of use and simplicity. However, you are free to choose
any programming language and any framework to implement the solution.

1. Visualize the complete historical table view of each tables in tabular format in stdout (hint: print your table)
2. Visualize the complete historical table view of the denormalized joined table in stdout by joining these three tables (hint: the join key lies in the `resources` section, please read carefully)
3. From result from point no 2, discuss how many transactions has been made, when did each of them occur, and how much the value of each transaction?  
   Transaction is defined as activity which change the balance of the savings account or credit used of the card
   

## Solution

Solution is based on Apache Spark 2.4.8
The main method is located [here](src/main/scala/app/DwhHistory.scala) and receives 2 parameters - `mainPath` and `SCD type`, e.g. `dwh/finance` (accounts, cards are within) and `type1` or `type2`.

### Lets discover data first

```html
=== Discover domain='accounts' ===
+----------+---+----------+--------+--------------------+-------+------------+-------------------+-------+-------+-----------------------+------------+------------------+
|id        |op |account_id|address |email               |name   |phone_number|dt                 |address|card_id|email                  |phone_number|savings_account_id|
+----------+---+----------+--------+--------------------+-------+------------+-------------------+-------+-------+-----------------------+------------+------------------+
|a1globalid|c  |a1        |New York|anthony@somebank.com|Anthony|12345678    |2020-01-01 10:30:00|null   |null   |null                   |null        |null              |
|a1globalid|u  |null      |null    |null                |null   |null        |2020-01-01 11:00:00|null   |null   |null                   |87654321    |null              |
|a1globalid|u  |null      |null    |null                |null   |null        |2020-01-01 18:00:00|null   |null   |null                   |null        |sa1               |
|a1globalid|u  |null      |null    |null                |null   |null        |2020-01-01 19:00:00|Jakarta|null   |anthony@anotherbank.com|null        |null              |
|a1globalid|u  |null      |null    |null                |null   |null        |2020-01-02 04:00:00|null   |c1     |null                   |null        |null              |
|a1globalid|u  |null      |null    |null                |null   |null        |2020-01-15 12:01:00|null   |       |null                   |null        |null              |
|a1globalid|u  |null      |null    |null                |null   |null        |2020-01-16 11:30:00|null   |c2     |null                   |null        |null              |
+----------+---+----------+--------+--------------------+-------+------------+-------------------+-------+-------+-----------------------+------------+------------------+

=== Discover domain='cards' ===
+----------+---+-------+-----------+-----------+-------------+-------+-------------------+-----------+------+
|id        |op |card_id|card_number|credit_used|monthly_limit|status |dt                 |credit_used|status|
+----------+---+-------+-----------+-----------+-------------+-------+-------------------+-----------+------+
|c1globalid|c  |c1     |11112222   |0          |30000        |PENDING|2020-01-02 04:00:00|null       |null  |
|c1globalid|u  |null   |null       |null       |null         |null   |2020-01-04 20:30:00|null       |ACTIVE|
|c1globalid|u  |null   |null       |null       |null         |null   |2020-01-06 15:30:00|12000      |null  |
|c1globalid|u  |null   |null       |null       |null         |null   |2020-01-07 21:00:00|19000      |null  |
|c1globalid|u  |null   |null       |null       |null         |null   |2020-01-10 14:00:00|0          |null  |
|c1globalid|u  |null   |null       |null       |null         |null   |2020-01-15 12:00:00|null       |CLOSED|
|c2globalid|c  |c2     |12123434   |0          |70000        |PENDING|2020-01-16 11:30:00|null       |null  |
|c2globalid|u  |null   |null       |null       |null         |null   |2020-01-18 01:00:00|null       |ACTIVE|
|c2globalid|u  |null   |null       |null       |null         |null   |2020-01-18 18:30:00|37000      |null  |
+----------+---+-------+-----------+-----------+-------------+-------+-------------------+-----------+------+

=== Discover domain='savings_accounts' ===
+-----------+---+-------+---------------------+------------------+------+-------------------+-------+---------------------+
|id         |op |balance|interest_rate_percent|savings_account_id|status|dt                 |balance|interest_rate_percent|
+-----------+---+-------+---------------------+------------------+------+-------------------+-------+---------------------+
|sa1globalid|c  |0      |1.5                  |sa1               |ACTIVE|2020-01-01 18:00:00|null   |null                 |
|sa1globalid|u  |null   |null                 |null              |null  |2020-01-02 12:00:00|15000  |null                 |
|sa1globalid|u  |null   |null                 |null              |null  |2020-01-04 20:31:00|null   |3.0                  |
|sa1globalid|u  |null   |null                 |null              |null  |2020-01-10 12:30:00|40000  |null                 |
|sa1globalid|u  |null   |null                 |null              |null  |2020-01-10 14:00:00|21000  |null                 |
|sa1globalid|u  |null   |null                 |null              |null  |2020-01-15 12:01:00|null   |1.5                  |
|sa1globalid|u  |null   |null                 |null              |null  |2020-01-18 01:01:00|null   |4.0                  |
|sa1globalid|u  |null   |null                 |null              |null  |2020-01-20 10:30:00|33000  |null                 |
+-----------+---+-------+---------------------+------------------+------+-------------------+-------+---------------------+
```

#### Notes:
- Here we have 'data' and 'set' separated by 'dt' field where:
'data' and 'set' are struct fields based on JSON which is dynamic and
lets say can be unpredictable on its further structure.
- Accounts updated with Card or SavingsAccount information at the same moment as its original table.
- Once card is CLOSED - accounts detached with that CLOSED card.

### There are 2 types of SCD that I have made:

- ### `SCD Type 1` - [method scd1](src/main/scala/app/DwhHistory.scala)
  
The main idea is to restore a state of the record by its events 
(record with 'c' op as an initial state, events are records with 'u' op)


### `[Task 1]` That how it is going to look like:
```html
=== ACCOUNTS === 
+----------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------+------------------+
|id        |ts           |data                                                                                                                                                          |card_id|savings_account_id|
+----------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------+------------------+
|a1globalid|1579163400000|{"name":"Anthony","phone_number":"87654321","email":"anthony@anotherbank.com","savings_account_id":"sa1","account_id":"a1","card_id":"c2","address":"Jakarta"}|c2     |sa1               |
+----------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------+------------------+

=== CARDS === 
+----------+-------------+-----------------------------------------------------------------------------------------------------+-------+
|id        |ts           |data                                                                                                 |card_id|
+----------+-------------+-----------------------------------------------------------------------------------------------------+-------+
|c2globalid|1579361400000|{"card_number":"12123434","credit_used":37000,"status":"ACTIVE","card_id":"c2","monthly_limit":70000}|c2     |
|c1globalid|1579078800000|{"card_number":"11112222","credit_used":0,"status":"CLOSED","card_id":"c1","monthly_limit":30000}    |c1     |
+----------+-------------+-----------------------------------------------------------------------------------------------------+-------+

=== SAVINGS ACCOUNT === 
+-----------+-------------+------------------------------------------------------------------------------------------+------------------+
|id         |ts           |data                                                                                      |savings_account_id|
+-----------+-------------+------------------------------------------------------------------------------------------+------------------+
|sa1globalid|1579505400000|{"balance":33000,"interest_rate_percent":4.0,"savings_account_id":"sa1","status":"ACTIVE"}|sa1               |
+-----------+-------------+------------------------------------------------------------------------------------------+------------------+
```

### `[Task 2]` That how it is going to look like:
```html
=== JOIN === 
+----------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------+------------------+----------+-------------+-----------------------------------------------------------------------------------------------------+-------+-----------+-------------+------------------------------------------------------------------------------------------+------------------+
|id        |ts           |data                                                                                                                                                          |card_id|savings_account_id|id        |ts           |data                                                                                                 |card_id|id         |ts           |data                                                                                      |savings_account_id|
+----------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------+------------------+----------+-------------+-----------------------------------------------------------------------------------------------------+-------+-----------+-------------+------------------------------------------------------------------------------------------+------------------+
|a1globalid|1579163400000|{"name":"Anthony","phone_number":"87654321","email":"anthony@anotherbank.com","savings_account_id":"sa1","account_id":"a1","card_id":"c2","address":"Jakarta"}|c2     |sa1               |c2globalid|1579361400000|{"card_number":"12123434","credit_used":37000,"status":"ACTIVE","card_id":"c2","monthly_limit":70000}|c2     |sa1globalid|1579505400000|{"balance":33000,"interest_rate_percent":4.0,"savings_account_id":"sa1","status":"ACTIVE"}|sa1               |
+----------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------+------------------+----------+-------------+-----------------------------------------------------------------------------------------------------+-------+-----------+-------------+------------------------------------------------------------------------------------------+------------------+
```


- ### `SCD Type 2` - [method scd2](src/main/scala/app/DwhHistory.scala)

The main idea is to use cascading update of the initial state of the record by its events keeping every update result as a record.
(record with 'c' op as an initial state, events are records with 'u' op)

### `[Task 1]`That how it is going to look like:
```html
=== ACCOUNTS === 
+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+------------------+
|id        |data                                                                                                                                                          |eff_from           |eff_to             |is_current|card_id|savings_account_id|
+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+------------------+
|a1globalid|{"name":"Anthony","phone_number":"12345678","email":"anthony@somebank.com","account_id":"a1","address":"New York"}                                            |2020-01-01 10:30:00|2020-01-01 11:00:00|0         |null   |null              |
|a1globalid|{"name":"Anthony","phone_number":"87654321","email":"anthony@somebank.com","account_id":"a1","address":"New York"}                                            |2020-01-01 11:00:00|2020-01-01 18:00:00|0         |null   |null              |
|a1globalid|{"name":"Anthony","phone_number":"87654321","email":"anthony@somebank.com","savings_account_id":"sa1","account_id":"a1","address":"New York"}                 |2020-01-01 18:00:00|2020-01-01 19:00:00|0         |null   |sa1               |
|a1globalid|{"name":"Anthony","phone_number":"87654321","email":"anthony@anotherbank.com","savings_account_id":"sa1","account_id":"a1","address":"Jakarta"}               |2020-01-01 19:00:00|2020-01-02 04:00:00|0         |null   |sa1               |
|a1globalid|{"name":"Anthony","phone_number":"87654321","email":"anthony@anotherbank.com","savings_account_id":"sa1","account_id":"a1","card_id":"c1","address":"Jakarta"}|2020-01-02 04:00:00|2020-01-15 12:01:00|0         |c1     |sa1               |
|a1globalid|{"name":"Anthony","phone_number":"87654321","email":"anthony@anotherbank.com","savings_account_id":"sa1","account_id":"a1","card_id":"","address":"Jakarta"}  |2020-01-15 12:01:00|2020-01-16 11:30:00|0         |       |sa1               |
|a1globalid|{"name":"Anthony","phone_number":"87654321","email":"anthony@anotherbank.com","savings_account_id":"sa1","account_id":"a1","card_id":"c2","address":"Jakarta"}|2020-01-16 11:30:00|2999-12-31 00:00:00|1         |c2     |sa1               |
+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+------------------+

=== CARDS === 
+----------+-----------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+
|id        |data                                                                                                 |eff_from           |eff_to             |is_current|card_id|
+----------+-----------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+
|c2globalid|{"card_number":"12123434","credit_used":0,"status":"PENDING","card_id":"c2","monthly_limit":70000}   |2020-01-16 11:30:00|2020-01-18 01:00:00|0         |c2     |
|c2globalid|{"card_number":"12123434","credit_used":0,"status":"ACTIVE","card_id":"c2","monthly_limit":70000}    |2020-01-18 01:00:00|2020-01-18 18:30:00|0         |c2     |
|c2globalid|{"card_number":"12123434","credit_used":37000,"status":"ACTIVE","card_id":"c2","monthly_limit":70000}|2020-01-18 18:30:00|2999-12-31 00:00:00|1         |c2     |
|c1globalid|{"card_number":"11112222","credit_used":0,"status":"PENDING","card_id":"c1","monthly_limit":30000}   |2020-01-02 04:00:00|2020-01-04 20:30:00|0         |c1     |
|c1globalid|{"card_number":"11112222","credit_used":0,"status":"ACTIVE","card_id":"c1","monthly_limit":30000}    |2020-01-04 20:30:00|2020-01-06 15:30:00|0         |c1     |
|c1globalid|{"card_number":"11112222","credit_used":12000,"status":"ACTIVE","card_id":"c1","monthly_limit":30000}|2020-01-06 15:30:00|2020-01-07 21:00:00|0         |c1     |
|c1globalid|{"card_number":"11112222","credit_used":19000,"status":"ACTIVE","card_id":"c1","monthly_limit":30000}|2020-01-07 21:00:00|2020-01-10 14:00:00|0         |c1     |
|c1globalid|{"card_number":"11112222","credit_used":0,"status":"ACTIVE","card_id":"c1","monthly_limit":30000}    |2020-01-10 14:00:00|2020-01-15 12:00:00|0         |c1     |
|c1globalid|{"card_number":"11112222","credit_used":0,"status":"CLOSED","card_id":"c1","monthly_limit":30000}    |2020-01-15 12:00:00|2999-12-31 00:00:00|1         |c1     |
+----------+-----------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+

=== SAVINGS ACCOUNT ===
+-----------+------------------------------------------------------------------------------------------+-------------------+-------------------+----------+------------------+
|id         |data                                                                                      |eff_from           |eff_to             |is_current|savings_account_id|
+-----------+------------------------------------------------------------------------------------------+-------------------+-------------------+----------+------------------+
|sa1globalid|{"balance":0,"interest_rate_percent":1.5,"savings_account_id":"sa1","status":"ACTIVE"}    |2020-01-01 18:00:00|2020-01-02 12:00:00|0         |sa1               |
|sa1globalid|{"balance":15000,"interest_rate_percent":1.5,"savings_account_id":"sa1","status":"ACTIVE"}|2020-01-02 12:00:00|2020-01-04 20:31:00|0         |sa1               |
|sa1globalid|{"balance":15000,"interest_rate_percent":3.0,"savings_account_id":"sa1","status":"ACTIVE"}|2020-01-04 20:31:00|2020-01-10 12:30:00|0         |sa1               |
|sa1globalid|{"balance":40000,"interest_rate_percent":3.0,"savings_account_id":"sa1","status":"ACTIVE"}|2020-01-10 12:30:00|2020-01-10 14:00:00|0         |sa1               |
|sa1globalid|{"balance":21000,"interest_rate_percent":3.0,"savings_account_id":"sa1","status":"ACTIVE"}|2020-01-10 14:00:00|2020-01-15 12:01:00|0         |sa1               |
|sa1globalid|{"balance":21000,"interest_rate_percent":1.5,"savings_account_id":"sa1","status":"ACTIVE"}|2020-01-15 12:01:00|2020-01-18 01:01:00|0         |sa1               |
|sa1globalid|{"balance":21000,"interest_rate_percent":4.0,"savings_account_id":"sa1","status":"ACTIVE"}|2020-01-18 01:01:00|2020-01-20 10:30:00|0         |sa1               |
|sa1globalid|{"balance":33000,"interest_rate_percent":4.0,"savings_account_id":"sa1","status":"ACTIVE"}|2020-01-20 10:30:00|2999-12-31 00:00:00|1         |sa1               |
+-----------+------------------------------------------------------------------------------------------+-------------------+-------------------+----------+------------------+
```

### `[Task 2]` That how it is going to look like:
```html
=== JOINs ===

==== ACCOUNTS to CARDS (on card_id and eff_from) ====
+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+------------------+----------+--------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+
|id        |data                                                                                                                                                          |eff_from           |eff_to             |is_current|card_id|savings_account_id|id        |data                                                                                              |eff_from           |eff_to             |is_current|card_id|
+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+------------------+----------+--------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+
|a1globalid|{"name":"Anthony","phone_number":"87654321","email":"anthony@anotherbank.com","savings_account_id":"sa1","account_id":"a1","card_id":"c1","address":"Jakarta"}|2020-01-02 04:00:00|2020-01-15 12:01:00|0         |c1     |sa1               |c1globalid|{"card_number":"11112222","credit_used":0,"status":"PENDING","card_id":"c1","monthly_limit":30000}|2020-01-02 04:00:00|2020-01-04 20:30:00|0         |c1     |
|a1globalid|{"name":"Anthony","phone_number":"87654321","email":"anthony@anotherbank.com","savings_account_id":"sa1","account_id":"a1","card_id":"c2","address":"Jakarta"}|2020-01-16 11:30:00|2999-12-31 00:00:00|1         |c2     |sa1               |c2globalid|{"card_number":"12123434","credit_used":0,"status":"PENDING","card_id":"c2","monthly_limit":70000}|2020-01-16 11:30:00|2020-01-18 01:00:00|0         |c2     |
+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+------------------+----------+--------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+

==== ACCOUNTS to SAVINGS_ACCOUNTS (on savings_accounts_id and eff_from) ====
+----------+------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+------------------+-----------+------------------------------------------------------------------------------------------+-------------------+-------------------+----------+------------------+
|id        |data                                                                                                                                                        |eff_from           |eff_to             |is_current|card_id|savings_account_id|id         |data                                                                                      |eff_from           |eff_to             |is_current|savings_account_id|
+----------+------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+------------------+-----------+------------------------------------------------------------------------------------------+-------------------+-------------------+----------+------------------+
|a1globalid|{"name":"Anthony","phone_number":"87654321","email":"anthony@somebank.com","savings_account_id":"sa1","account_id":"a1","address":"New York"}               |2020-01-01 18:00:00|2020-01-01 19:00:00|0         |null   |sa1               |sa1globalid|{"balance":0,"interest_rate_percent":1.5,"savings_account_id":"sa1","status":"ACTIVE"}    |2020-01-01 18:00:00|2020-01-02 12:00:00|0         |sa1               |
|a1globalid|{"name":"Anthony","phone_number":"87654321","email":"anthony@anotherbank.com","savings_account_id":"sa1","account_id":"a1","card_id":"","address":"Jakarta"}|2020-01-15 12:01:00|2020-01-16 11:30:00|0         |       |sa1               |sa1globalid|{"balance":21000,"interest_rate_percent":1.5,"savings_account_id":"sa1","status":"ACTIVE"}|2020-01-15 12:01:00|2020-01-18 01:01:00|0         |sa1               |
+----------+------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+----------+-------+------------------+-----------+------------------------------------------------------------------------------------------+-------------------+-------------------+----------+------------------+
```

### `[Task 3]` Here are the calculations:

We have 24 records across all 3 tables.
If we consider records appearance in more than one table at the same time as a transaction then we have 20 transactions:
- 2 transactions are about cards opening - 2 records in CARDS table (c1 card OPENED, c2 card OPENED) + ACCOUNTS updated 2 times -> 4 records;
- 1 transaction is about savings_account opening - 1 record in SAVINGS_ACCOUNTS + ACCOUNTS updated 1 time -> 2 records;
- 1 transaction is about cards closing - 1 record in ACCOUNTS + 1 record in SAVINGS_ACCOUNTS -> 2 records 
- 16 independent transactions within each table in total.

Another assumption is that when c1 card was closed and account with savings_accounts were impacted - should be considered as 1 transaction (even if there is 1 minute difference) 
then we have 15 independent transactions and 24 records.