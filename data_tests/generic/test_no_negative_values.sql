{% test no_negative_values(model, column) %}

{#-
This test checks if there are any negative values in a specified column of a model.
-#}

WITH negative_values_check AS (
    SELECT
        {{ column }} AS value,
        COUNT(*) AS count_negative_values
    FROM {{ model }}
    WHERE {{ column }} < 0
    GROUP BY {{ column }}
)

SELECT *
FROM negative_values_check
WHERE count_negative_values > 0

{% endtest %}