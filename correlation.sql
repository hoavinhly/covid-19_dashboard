WITH
  sub_data AS (
  SELECT
    CASE
      WHEN country_name = 'United States of America' THEN "US"
    ELSE
    country_name
  END
    AS country_name,
    date,
    cumulative_vaccine_doses_administered,
    MAX(cumulative_vaccine_doses_administered) OVER (PARTITION BY country_name ORDER BY date ) AS vaccinated,
    new_deceased
  FROM
    `bigquery-public-data.covid19_open_data.covid19_open_data`
  WHERE
    aggregation_level = 0 ),
  jhu_data AS (
  SELECT
    date,
    SUM(deaths) AS deaths,
    country_region
  FROM
    `bigquery-public-data.covid19_jhu_csse.summary`
  GROUP BY
    date,
    country_region )
SELECT
  sub_data.new_deceased,
  sub_data.vaccinated,
  jhu_data.date,
  jhu_data.deaths,
  jhu_data.country_region
FROM
  jhu_data
LEFT JOIN
  sub_data
ON
  sub_data.country_name = jhu_data.country_region
  AND sub_data.date = jhu_data.date

ORDER BY
  date DESC
    
