SELECT id, name FROM tasks
WHERE
{% if name %}
    name LIKE {{ name }} AND
{% endif %}
TRUE;
