from deltalake import Field


class DField:
    def __init__(self, field: Field):
        self.field = field

    @property
    def odd_metadata(self):
        return self.field.metadata

    @property
    def name(self):
        return self.field.name

    @property
    def type(self):
        return self.type

    @property
    def nullable(self):
        return self.nullable
