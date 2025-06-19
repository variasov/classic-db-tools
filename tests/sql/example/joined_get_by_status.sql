SELECT
    tasks.id,
    tasks.name
    {% if status %}, task_status.status {% endif %}
FROM tasks
{% if status %}
    INNER JOIN task_status ON tasks.id = task_status.task_id
{% endif %}
WHERE
    {% if status %} task_status.status LIKE {{ status }} AND {% endif %}
    TRUE;
