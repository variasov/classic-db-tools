from dataclasses import dataclass, field
import inspect


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


@dataclass
class Dude:
    id: int
    name: str


@dataclass
class Cake:
    id: int
    name: str
    slices: int
    eaten_by: list[Dude] = field(default_factory=list)


def test_map(rows):
    # разбор полей курсора

    cake_signature = {
        name: param
        for name, param in inspect.signature(Cake)
    }

    for desc in cursor.description:
        cake_signature[desc[0]]


    id = 0
    name = 1
    slices = 2
    dude_id = 3
    dude_name = 4

    cakes = {}
    dudes = {}
    for row in rows:
        cake = cakes.get(row[id])
        if cake is None:
            cake = cakes[row[id]] = Cake(
                id=row[id],
                name=row[name],
                slices=row[slices],
            )

        dude = dudes.get(row[dude_id])
        if dude is None:
            dude = dudes[row[dude_id]] = Dude(
                id=row[dude_id],
                name=row[dude_name],
            )

        cake.eaten_by.append(dude)

    return cakes
