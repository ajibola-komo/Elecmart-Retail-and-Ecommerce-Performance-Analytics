{% test expression_is_true(model, expression, column_name=None) %}

SELECT *
FROM {{ model }}
WHERE NOT ({{ expression }})

{% endtest %}