SELECT
    {% if status %} COUNT(tasks.id)  {% else %} tasks.id {% endif %}
FROM tasks
{% if status %} JOIN task_status ON task_status.task_id = tasks.id {% endif %}
WHERE
{% if status %} task_status.status LIKE {{ status }} AND {% endif %}
TRUE;
