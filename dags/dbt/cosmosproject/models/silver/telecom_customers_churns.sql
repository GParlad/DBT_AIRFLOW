{{ dbt_utils.union_relations(
    relations=[ref('telecom_customer_churn_1'), ref('telecom_customer_churn_2'), ref('telecom_customer_churn_3')]
) }}