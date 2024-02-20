SELECT 
    ZIP_CODE,
    COUNT(CUSTOMER_ID) as total_customers,
    AVG(AGE) as average_age, 
    AVG(NUMBER_OF_DEPENDENTS) as average_no_dependents_per_customer,
    ((sum(case when GENDER = 'Male' then 1 else 0 end)/count(*))*100) as male_percentage,
    ((sum(case when GENDER = 'Female' then 1 else 0 end)/count(*))*100) as female_percentage,
    ((sum(case when PHONE_SERVICE = 'True' then 1 else 0 end)/count(*))*100) as phone_service_percentage,
    ((sum(case when INTERNET_SERVICE = 'True' then 1 else 0 end)/count(*))*100) as internet_service_percentage,
    ((sum(case when INTERNET_SERVICE = 'True' or PHONE_SERVICE = 'True' then 1 else 0 end)/max(population))*100) as service_percentage_over_population
FROM {{ ref('full_telecom_customers') }}
GROUP BY ZIP_CODE