SELECT id, name FROM tasks
WHERE
{% if name %}
    name LIKE {{ name }} AND
{% endif %}
TRUE;
-- упростить (проверить с кучей фильтров, сортировок, таких как IN, FILTER, LIKE
--name LIKE {{ name }} AND # if name