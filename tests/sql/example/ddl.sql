CREATE TABLE tasks (
    id serial PRIMARY KEY,
    name varchar NULL,
    value varchar NULL,
    created_at timestamptz DEFAULT now()
);

CREATE TABLE task_status (
    id serial PRIMARY KEY,
    title varchar NULL,
    task_id int NULL,
    created_at timestamptz DEFAULT now()
)
