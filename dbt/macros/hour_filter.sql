{#
  증분 모델이 다시 만들 시간 범위.
  - --vars로 start/end를 주면 그 범위 [start, end) (재처리 뒤 재빌드)
  - 아니면 대상 테이블의 마지막 시간부터 (그 시간에 늦게 온 행을 반영한다)
  insert_overwrite 전략이라 새로 계산된 시간(파티션)만 통째로 바뀐다.
#}
{% macro hour_filter(col) -%}
  {%- if var('start') and var('end') -%}
    {{ col }} >= toDateTime('{{ var("start") }}') AND {{ col }} < toDateTime('{{ var("end") }}')
  {%- else -%}
    {{ col }} >= (SELECT max(hour) FROM {{ this }})
  {%- endif -%}
{%- endmacro %}
