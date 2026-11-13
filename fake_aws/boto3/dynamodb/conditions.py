"""Subconjunto fiel de boto3.dynamodb.conditions (Key/Attr + & | ~) evaluable en memoria."""


class _Cond:
    def __and__(self, o): return _Bool("AND", self, o)
    def __or__(self, o): return _Bool("OR", self, o)
    def __invert__(self): return _Not(self)


class _Bool(_Cond):
    def __init__(self, op, a, b): self.op, self.a, self.b = op, a, b
    def ev(self, item): return (self.a.ev(item) and self.b.ev(item)) if self.op == "AND" else (self.a.ev(item) or self.b.ev(item))
    def leaves(self): return self.a.leaves() + self.b.leaves() if self.op == "AND" else [("OR", self)]


class _Not(_Cond):
    def __init__(self, a): self.a = a
    def ev(self, item): return not self.a.ev(item)
    def leaves(self): return [("NOT", self)]


class _Leaf(_Cond):
    def __init__(self, attr, op, *vals): self.attr, self.op, self.vals = attr, op, vals
    def leaves(self): return [("LEAF", self)]
    def ev(self, item):
        has = self.attr in item
        v = item.get(self.attr)
        if self.op == "exists": return has
        if self.op == "not_exists": return not has
        if not has: return self.op == "ne"
        a = self.vals[0] if self.vals else None
        try:
            return {"eq": lambda: v == a, "ne": lambda: v != a, "lt": lambda: v < a, "lte": lambda: v <= a,
                    "gt": lambda: v > a, "gte": lambda: v >= a,
                    "begins_with": lambda: isinstance(v, str) and v.startswith(a),
                    "between": lambda: self.vals[0] <= v <= self.vals[1],
                    "contains": lambda: a in v}[self.op]()
        except TypeError:
            return False


class _Attr:
    def __init__(self, name): self.name = name
    def eq(self, v): return _Leaf(self.name, "eq", v)
    def ne(self, v): return _Leaf(self.name, "ne", v)
    def lt(self, v): return _Leaf(self.name, "lt", v)
    def lte(self, v): return _Leaf(self.name, "lte", v)
    def gt(self, v): return _Leaf(self.name, "gt", v)
    def gte(self, v): return _Leaf(self.name, "gte", v)
    def between(self, a, b): return _Leaf(self.name, "between", a, b)
    def begins_with(self, v): return _Leaf(self.name, "begins_with", v)
    def contains(self, v): return _Leaf(self.name, "contains", v)
    def exists(self): return _Leaf(self.name, "exists")
    def not_exists(self): return _Leaf(self.name, "not_exists")


class Key(_Attr):
    pass


class Attr(_Attr):
    pass
