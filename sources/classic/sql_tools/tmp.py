data = [
    {
        "id": 0,
        "name": "Carrot cake",
        "slices": 8,
        "dude_id": 0,
        "dude_name": "Ahsoka"
    },
    {
        "id": 0,
        "name": "Carrot cake",
        "slices": 8,
        "dude_id": 3,
        "dude_name": "CT-7567 Rex"
    }
]

result = [{
    "id": 0,
    "name": "Carrot cake",
    "slices": 8,
    "eaten_by":
        [{"id": 0, "name": "Ahsoka"},
         {"id": 3, "name": "CT-7567 Rex"}]
}]


def test_map(rows):
    cakes = {}
    dudes = {}
    for row in rows:
        cake = cakes.get(row["id"])
        if cake is None:
            cake = cakes[row["id"]] = {
                "id": row["id"],
                "name": row["name"],
                "slices": row["slices"],
                "eaten_by": [],
            }

        dude = dudes.get(row["dude_id"])
        if dude is None:
            dude = dudes[row["dude_id"]] = {
                "id": row["dude_id"],
                "name": row["dude_name"],
            }

        cake["eaten_by"].append(dude)

    return cakes
