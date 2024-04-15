{#
    this macro returns the description of the payment type
#}

{% macro get_payment_type_description(payment_type) -%}

    case {{ payment_type }}
        when 1 then 'credit card'
        when 2 then 'cash'
        when 3 then 'no charge'
        when 4 then 'dispute'
        when 5 then 'unknown'
        when 6 then 'voided trip'
    end
{%- endmacro %}