{% macro cleanse_text_field(mName,cName) %}

CAST(CASE
  WHEN TRIM({{ cName }}) = '"' THEN NULL
  WHEN TRIM({{ cName }}) = ',' THEN NULL
  WHEN TRIM({{ cName }}) = ';' THEN NULL
  WHEN TRIM({{ cName }}) = '.' THEN NULL
  ELSE NULLIF(TRIM({{ cName }}), '')
END AS string) 

{% endmacro %}