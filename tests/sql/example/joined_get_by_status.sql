SELECT
    tasks.id,
    tasks.name
    {% if status %}, task_status.status {% endif %}
FROM tasks
{% if status %} JOIN task_status ON task_status.task_id = tasks.id {% endif %}
WHERE
{% if status %} task_status.status LIKE {{ status }} AND {% endif %}
TRUE;
