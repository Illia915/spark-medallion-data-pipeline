"""
1. Check for dublicated transection id -> Треба написати функцію яка бере дані за найпершим order_date 
2. Chek for record where ship_date < order_date -> Продумай логіку 
3. Maybe clean customer_id CUST*** -> *** and if dome record start not from CUST -> ВСЕ ЧЕТКО
4. Check for diffrents gender records .groupBy(gender, count()) -> ПРИВЕСТИ В 1 ФОРМАТ
5. CHECK IF ANY RECORD STARTS NOT WITH PROD -> ВСЕ ЧЕТКО
6. Check for diff types of product_category -> ВСЕ ЧЕТКО
7. Check if quantity parametr is > 0 -> Є значення <= 0 
8. Check for unvalid unit_price -> Обдумай обробку 
9. Check if discount_pct < 100 anf for invalid records  -> Є значенння більші 100 і менше 0
10. Also dicide what to fo with null values 
11. Customer age <= 0 | age > 110
"""

#1. check for duplicate tr_id 
bronze_df_duplicated_records = (
    bronze_df.groupBy("transaction_id")
    .agg(F.count("transaction_id").alias("amount_of_records"))
    .filter(F.col("amount_of_records") > 1)
)

#2. Chek for record where ship_date < order_date 
bronze_df_date_unmatched = (
    bronze_df.filter(
        F.col("order_date") > F.col("ship_date")
 )
)

#4. Gender exploration 
bronze_df_gender = (
    bronze_df.groupBy("gender")
    .count().alias("amount")
)

#6. Check for diff types of product_category
bronze_df_prd_ctg = (
    bronze_df.groupBy("product_category")
    .count().alias("amount")
)

#7. Check if quantity parametr is > 0 
bronze_df_quantity = (
    bronze_df.filter(F.col("quantity") <= 0)
)

#9. Check if discount_pct < 100 anf for invalid records 
bronze_df_discount_pct = (
        bronze_df.filter(
        (F.col("discount_pct") >= 100) | (F.col("discount_pct") < 0)
    )
)

"""
1. customer_age
2. gender
3. discount_pct
4. order_status
"""