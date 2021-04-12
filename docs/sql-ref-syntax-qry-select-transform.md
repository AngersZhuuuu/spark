---
layout: global
title: TRANSFORM
displayTitle: TRANSFORM
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

### Description

The `TRANSFORM` clause is used to specify a Hive-style transform (`SELECT TRANSFORM`/`MAP`/`REDUCE`)
query specification to transform the input by running a specified script. Users can
plug in their own custom mappers or reducers in the data stream by using features natively supported
in the Spark SQL. In order to run a custom mapper script `map_script` or a custom
reducer script `reduce_script` the user can issue the command which uses the `TRANSFORM`
clause to embed the mapper or the reducer scripts.

### Syntax

```sql
SELECT { TRANSFORM ( named_expression [ , ... ] ) | MAP named_expression [ , ... ] | REDUCE named_expression [ , ... ] }
    [ rowFormat ]
    [ RECORDWRITER recordWriter_class ]
    USING script [ AS ( [ col_name [ col_type ] ] [ , ... ] ) ]
    [ rowFormat ]
    [ RECORDREADER recordReader_class ]
```

While `rowFormat` are defined as
```sql
{ ROW FORMAT SERDE serde_class [ WITH SERDEPROPERTIES serde_props ] | 
    ROW FORMAT DELIMITED
        [ FIELDS TERMINATED BY fields_terminated_char [ ESCAPED BY escapedBy ] ]
        [ COLLECTION ITEMS TERMINATED BY collectionItemsTerminatedBy ]
        [ MAP KEYS TERMINATED BY keysTerminatedBy ]
        [ LINES TERMINATED BY linesSeparatedBy ]
        [ NULL DEFINED AS nullDefinedAs ] }
```

### Parameters

* **named_expression**

    An expression with an assigned name. In general, it denotes a column expression.

    **Syntax:** `expression [AS] [alias]`

* **row_format**    

    Spark uses the `SERDE` clause to specify a custom SerDe for one table. Otherwise, use the `DELIMITED` clause to use the native SerDe and specify the delimiter, escape character, null character and so on.

* **serde_class**

    Specifies a fully-qualified class name of a custom SerDe.

* **serde_props**

    A list of key-value pairs that is used to tag the SerDe definition.

* **FIELDS TERMINATED BY**

    Used to define a column separator.
    
* **COLLECTION ITEMS TERMINATED BY**

    Used to define a collection item separator.
   
* **MAP KEYS TERMINATED BY**

    Used to define a map key separator.
    
* **LINES TERMINATED BY**

    Used to define a row separator.
    
* **NULL DEFINED AS**

    Used to define the specific value for NULL.
    
* **ESCAPED BY**

    Used for escape mechanism.

* **RECORDREADER**

    Specifies a custom RecordReader for one table.

* **RECORDWRITER**

    Specifies a custom RecordWriter for one table.

* **recordReader_class**

    Specifies a fully-qualified class name of a custom RecordReader. A default value is `org.apache.hadoop.hive.ql.exec.TextRecordReader`.

* **recordWriter_class**

    Specifies a fully-qualified class name of a custom RecordWriter. A default value is `org.apache.hadoop.hive.ql.exec.TextRecordWriter`.

* **script**

    Specifies a command to process data.

### Serde behavior

Spark uses Hive Serde `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` by default, columns will be transformed
to `STRING` and combined by tabs before feeding to the user script. All `NULL` values will be converted
to the literal string `"\N"` in order to differentiate `NULL` values from empty strings. The standard output of the
user script will be treated as TAB-separated STRING columns, any cell containing only `"\N"` will be re-interpreted
as a `NULL`, and then the resulting STRING column will be cast to the data type specified in the table declaration
in the usual way. If the actual number of output columns is less than the number of specified output columns,
insufficient output columns will be supplemented with `NULL`. If the actual number of output columns is more than the
number of specified output columns, the output columns will only select the corresponding columns and the remaining part
will be discarded. If there is no `AS` clause after `USING my_script`, Spark assumes that the output of the script contains 2 parts:

   1. key: which is before the first tab.
   2. value: which is the rest after the first tab.

Note that this is different from specifying an `AS key, value` because in that case, the value will only contain the portion
between the first tab and the second tab if there are multiple tabs. 
User scripts can output debug information to standard error which will be shown on the task detail
page on Spark. These defaults can be overridden with `ROW FORMAT SERDE` or `ROW FORMAT DELIMITED`. 

### Examples

```sql
CREATE TABLE person (zip_code INT, name STRING, age INT);
INSERT INTO person VALUES
    (94588, 'Zen Hui', 50),
    (94588, 'Dan Li', 18),
    (94588, 'Anil K', 27),
    (94588, 'John V', NULL),
    (94511, 'David K', 42),
    (94511, 'Aryan B.', 18),
    (94511, 'Lalit B.', NULL);

-- With specified output without data type
SELECT TRANSFORM(zip_code, name, age)
   USING 'cat' AS (a, b, c)
FROM person
WHERE zip_code > 94511;
+-------+---------+-----+
|    a  |        b|    c|
+-------+---------+-----+
|  94588|   Anil K|   27|
|  94588|   John V| NULL|
|  94588|  Zen Hui|   50|
|  94588|   Dan Li|   18|
+-------+---------+-----+

-- With specified output with data type
SELECT TRANSFORM(zip_code, name, age)
   USING 'cat' AS (a STRING, b STRING, c STRING)
FROM person
WHERE zip_code > 94511;
+-------+---------+-----+
|    a  |        b|    c|
+-------+---------+-----+
|  94588|   Anil K|   27|
|  94588|   John V| NULL|
|  94588|  Zen Hui|   50|
|  94588|   Dan Li|   18|
+-------+---------+-----+

-- Using ROW FORMAT DELIMITED
SELECT TRANSFORM(name, age)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    NULL DEFINED AS 'NULL'
    USING 'cat' AS (name_age string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '@'
    LINES TERMINATED BY '\n'
    NULL DEFINED AS 'NULL'
FROM person;
+---------------+
|       name_age|
+---------------+
|      Anil K,27|
|    John V,null|
|     ryan B.,18|
|     David K,42|
|     Zen Hui,50|
|      Dan Li,18|
|  Lalit B.,null|
+---------------+

-- Using Hive Serde
SELECT TRANSFORM(zip_code, name, age)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
      'field.delim' = '\t'
    )
    USING 'cat' AS (a STRING, b STRING, c STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
      'field.delim' = '\t'
    )
FROM person
WHERE zip_code > 94511;
+-------+---------+-----+
|    a  |        b|    c|
+-------+---------+-----+
|  94588|   Anil K|   27|
|  94588|   John V| NULL|
|  94588|  Zen Hui|   50|
|  94588|   Dan Li|   18|
+-------+---------+-----+

-- Schema-less mode
SELECT TRANSFORM(zip_code, name, age)
    USING 'cat'
FROM person
WHERE zip_code > 94500;
+-------+---------------------+
|    key|                value|
+-------+---------------------+
|  94588|	  Anil K    27|
|  94588|	  John V    \N|
|  94511|	Aryan B.    18|
|  94511|	 David K    42|
|  94588|	 Zen Hui    50|
|  94588|	  Dan Li    18|
|  94511|	Lalit B.    \N|
+-------+---------------------+
```

### Related Statements

* [SELECT Main](sql-ref-syntax-qry-select.html)
* [WHERE Clause](sql-ref-syntax-qry-select-where.html)
* [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
* [HAVING Clause](sql-ref-syntax-qry-select-having.html)
* [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
* [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
* [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
* [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
* [CASE Clause](sql-ref-syntax-qry-select-case.html)
* [PIVOT Clause](sql-ref-syntax-qry-select-pivot.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
