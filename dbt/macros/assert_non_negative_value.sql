{#
  =============================================================================
  assert_non_negative_value — Custom Generic dbt Test Macro
  =============================================================================

  WHAT THIS TEST DOES:
    Returns rows where column_name < 0 (strictly negative values).
    Used to validate that unit_price and revenue are >= 0.
    Zero is allowed (free/promotional items have unit_price = 0).

  HOW TO INVOKE IN schema.yml:
    columns:
      - name: unit_price
        tests:
          - assert_non_negative_value:
              severity: warn

  ARGUMENTS:
    model       — injected by dbt; references the source/model under test
    column_name — injected by dbt; the column being tested

  =============================================================================
#}

{% test assert_non_negative_value(model, column_name) %}

    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE
        {{ column_name }} IS NOT NULL
        AND {{ column_name }} < 0

{% endtest %}
