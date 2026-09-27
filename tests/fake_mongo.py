"""A small in-memory stand-in for the motor collections the app uses.

Enough of the Mongo surface to exercise real code paths in tests without a
running server: query matching (equality, $regex, $in), projections, sort,
$set / $push updates, and the insert/delete calls. Deliberately not a general
Mongo emulator — it implements what this codebase actually calls, and raises
on anything it does not understand rather than quietly returning wrong data.
"""

import copy
import re


def _matches(doc, query):
    """True when `doc` satisfies every clause of `query`."""
    for key, want in (query or {}).items():
        have = doc.get(key)
        if isinstance(want, dict):
            if "$regex" in want:
                flags = re.I if "i" in want.get("$options", "") else 0
                if not re.match(want["$regex"], str(have or ""), flags):
                    return False
            elif "$in" in want:
                if have not in want["$in"]:
                    return False
            elif "$ne" in want:
                if have == want["$ne"]:
                    return False
            else:
                raise NotImplementedError(f"query operator in {want!r}")
        elif have != want:
            return False
    return True


def _project(doc, projection):
    """Apply a Mongo-style projection, honouring dotted exclusions."""
    if not projection:
        return copy.deepcopy(doc)
    excludes = {k for k, v in projection.items() if v == 0}
    includes = {k for k, v in projection.items() if v == 1}
    out = copy.deepcopy(doc)
    if includes:
        out = {k: v for k, v in out.items() if k in includes or k == "id"}
    for key in excludes:
        if "." in key:
            head, tail = key.split(".", 1)
            if isinstance(out.get(head), dict):
                out[head].pop(tail, None)
        else:
            out.pop(key, None)
    return out


def _sorted(docs, spec):
    """Sort by a motor sort spec: a [(field, direction)] list."""
    for field, direction in reversed(list(spec or [])):
        docs = sorted(
            docs,
            key=lambda d: (d.get(field) is None, d.get(field)),
            reverse=direction < 0,
        )
    return docs


class _Cursor:
    def __init__(self, docs, projection=None):
        self._docs = docs
        self._projection = projection
        self._sort = None

    def sort(self, field, direction=1):
        self._sort = [(field, direction)]
        return self

    async def to_list(self, length=None):
        docs = _sorted(self._docs, self._sort) if self._sort else list(self._docs)
        if length is not None:
            docs = docs[:length]
        return [_project(d, self._projection) for d in docs]


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = [copy.deepcopy(d) for d in (docs or [])]
        self.indexes = []

    # -- reads -----------------------------------------------------------
    async def find_one(self, query=None, projection=None, sort=None):
        candidates = [d for d in self.docs if _matches(d, query)]
        if sort:
            candidates = _sorted(candidates, sort)
        return _project(candidates[0], projection) if candidates else None

    def find(self, query=None, projection=None, sort=None):
        matched = [d for d in self.docs if _matches(d, query)]
        if sort:
            matched = _sorted(matched, sort)
        return _Cursor(matched, projection)

    async def count_documents(self, query=None):
        return sum(1 for d in self.docs if _matches(d, query))

    # -- writes ----------------------------------------------------------
    async def insert_one(self, doc):
        self.docs.append(copy.deepcopy(doc))
        return type("R", (), {"inserted_id": len(self.docs)})()

    async def update_one(self, query, update):
        for doc in self.docs:
            if _matches(doc, query):
                for field, value in (update.get("$set") or {}).items():
                    doc[field] = copy.deepcopy(value)
                for field, value in (update.get("$push") or {}).items():
                    doc.setdefault(field, []).append(copy.deepcopy(value))
                return type("R", (), {"matched_count": 1, "modified_count": 1})()
        return type("R", (), {"matched_count": 0, "modified_count": 0})()

    async def replace_one(self, query, doc, upsert=False):
        for i, existing in enumerate(self.docs):
            if _matches(existing, query):
                self.docs[i] = copy.deepcopy(doc)
                return type("R", (), {"matched_count": 1})()
        if upsert:
            self.docs.append(copy.deepcopy(doc))
        return type("R", (), {"matched_count": 0})()

    async def delete_one(self, query):
        for i, doc in enumerate(self.docs):
            if _matches(doc, query):
                del self.docs[i]
                return type("R", (), {"deleted_count": 1})()
        return type("R", (), {"deleted_count": 0})()

    async def delete_many(self, query):
        before = len(self.docs)
        self.docs = [d for d in self.docs if not _matches(d, query)]
        return type("R", (), {"deleted_count": before - len(self.docs)})()

    # -- indexes ---------------------------------------------------------
    async def create_index(self, keys, unique=False, background=False):
        """Recorded, not enforced — tests assert the specs, not the lookups."""
        self.indexes.append({"keys": list(keys), "unique": bool(unique)})
        return "_".join(f"{k}_{d}" for k, d in keys)


class FakeDB:
    """Attribute access mints collections on demand, as motor's db does."""

    def __init__(self, **collections):
        self._cols = {name: FakeCollection(docs)
                      for name, docs in collections.items()}

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self._cols.setdefault(name, FakeCollection())

    def __getitem__(self, name):
        """motor exposes collections by subscript as well as by attribute."""
        return self._cols.setdefault(name, FakeCollection())
