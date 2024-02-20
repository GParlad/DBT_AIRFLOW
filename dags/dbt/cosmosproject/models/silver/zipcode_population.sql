SELECT ZIP_CODE, POPULATION
FROM {{ ref('telecom_zipcode_population') }}