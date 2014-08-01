Map-Reduce-Data-Pivot
=====================

This is a small Map Reduce project to create a general purpose process for pivoting data sets for analysis.

The idea would be familiar to anyone who has used Pivot tables in spreadsheet applications:

Take data that looks like this:

Store,Product,Number  
---
Store-1, Hats, 4  
Store-1, Shoes, 2  
Store-1, Gloves, 1  
Store-2, Hats, 0  
Store-2, Shoes, 9  
Store-2, Gloves, 4  
Store-3, Hats, 2  
Store-3, Shoes, 0  
Store-3, Gloves, 3  
Store-3, Umbrellas, 1  

and turn it into this :

Store,  Hats, Shoes, Gloves, Umbrellas  
---
Store-1, 4,    2,     1,      ?  
Store-2, 0,    9,     4,      ?  
Store-3, 2,    0,     3,      1  

and be able to that for millions of records and large numbers of products.


Features
--------

This will include the capacity to set how the data is aggregated, either by summing, averaging or simply counting the number of records.
