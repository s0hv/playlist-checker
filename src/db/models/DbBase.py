from dataclasses import dataclass


@dataclass(eq=False)
class DbBase:
    id: int

    def __eq__(self, other):
        if isinstance(other, DbBase):
            return other.id == self.id

        return False
