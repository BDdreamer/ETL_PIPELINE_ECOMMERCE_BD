{#
  =============================================================================
  assert_composite_unique — Custom Generic dbt Test Macro
  =============================================================================

  WHAT THIS TEST DOES:
    Returns rows where the combination of specified columns is duplicated.
    Used to validate composite key uniqueness — e.g., the Silver deduplication
    key (invoice_no, stock_code, invoice_date, quantity).

  HOW TO INVOKE IN schema.yml:
    The test is declared at the table level (not column level) because it
    spans multiple columns:

    sources:
      - name: silver
        tables:
          - name: transactions
            tests:
              - assert_composite_unique:
                  columns: ["invoice_no", "stock_code", "invoice_date", "quantity"]
                  severity: error

  ARGUMENTS:
    model   — injected by dbt; references the source/model under test
    columns — list of column names that form the composite key

  HOW IT WORKS:
    Groups by all specified columns and returns groups with COUNT(*) > 1.
    dbt counts the returned rows — if any duplicates exist, the test fails.

  =============================================================================
#}

{% test assert_composite_unique(model, columns) %}

    SELECT
        {% for col in columns %}
        {{ col }}{% if not loop.last %},{% endif %}
        {% endfor %},
        COUNT(*) AS duplicate_count
    FROM {{ model }}
    GROUP BY
        {% for col in columns %}
        {{ col }}{% if not loop.last %},{% endif %}
        {% endfor %}
    HAVING COUNT(*) > 1

{% endtest %}
