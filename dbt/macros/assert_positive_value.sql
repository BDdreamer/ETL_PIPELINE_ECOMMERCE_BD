{#
  =============================================================================
  assert_positive_value — Custom Generic dbt Test Macro
  =============================================================================

  WHAT IS A GENERIC TEST?
    In dbt, a generic test is a macro that returns a SELECT query of *failing
    rows*. dbt executes the query and counts the returned rows. If the count
    is greater than zero, the test fails. If the count is zero, the test passes.

    You do NOT return True/False — you return the rows that violate the
    condition. dbt does the counting and reporting.

  WHAT THIS TEST DOES:
    Returns rows where column_name <= 0 (i.e., zero or negative values).
    Used to validate that quantity and revenue are strictly positive.

  HOW TO INVOKE IN schema.yml:
    columns:
      - name: quantity
        tests:
          - assert_positive_value:
              severity: error

      - name: revenue
        tests:
          - assert_positive_value:
              severity: warn

  ARGUMENTS:
    model       — injected by dbt; references the source/model under test
    column_name — injected by dbt; the column being tested

  =============================================================================
#}

{% test assert_positive_value(model, column_name) %}

    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE
        {{ column_name }} IS NOT NULL
        AND {{ column_name }} <= 0

{% endtest %}
