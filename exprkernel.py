"""Expression kernel: name lookup, scalar operators, short-circuit logic,
aggregate argument evaluation and subquery consumption.

evaluate(e, env, params, group=None, aggregating=False) returns a generator.
Start it with next(), answer its requests with send(), and inject business
failures with throw(protocol.Fault(message)). The scalar result is delivered
as StopIteration.value. The kernel never sees tables and never creates or
calls relation executors; subqueries are consumed only through these requests:

* ("trace", event)              -> respond None
* ("open", query, outer, existence) -> respond an opaque relation handle
* ("next", handle)              -> respond ("row", row) or ("end",)
* ("close", handle)             -> respond None

Every successful open is matched by exactly one close, on normal completion,
early hits and faults alike. The kernel does not interpret handles.
"""
from protocol import Fault


def truth(value):
    if value is None:
        return None
    if type(value) is not bool:
        raise Fault('BOOL_REQUIRED')
    return value


def column(env, ref):
    for frame in env:
        if len(ref) == 2:
            hits = [row[i] for alias, names, row in frame
                    for i, name in enumerate(names) if name == ref[1]]
        else:
            bindings = [b for b in frame if b[0] == ref[1]]
            if not bindings:
                continue
            if len(bindings) != 1:
                raise Fault('AMBIGUOUS:' + '.'.join(ref[1:]))
            _, names, row = bindings[0]
            hits = [row[i] for i, name in enumerate(names) if name == ref[2]]
            if not hits:
                raise Fault('UNKNOWN:' + '.'.join(ref[1:]))
        if hits:
            if len(hits) != 1:
                raise Fault('AMBIGUOUS:' + '.'.join(ref[1:]))
            return hits[0]
    raise Fault('UNKNOWN:' + '.'.join(ref[1:]))


def evaluate(e, env, params, group=None, aggregating=False):
    tag = e[0]
    if tag == 'lit':
        return e[1]
    if tag == 'col':
        return column(env, e)
    if tag == 'param':
        if e[1] not in params:
            raise Fault('PARAM:' + e[1])
        return params[e[1]]
    if tag == 'if':
        cond = yield from evaluate(e[1], env, params, group, aggregating)
        branch = e[2] if truth(cond) is True else e[3]
        return (yield from evaluate(branch, env, params, group, aggregating))
    if tag == 'coalesce':
        for part in e[1:]:
            value = yield from evaluate(part, env, params, group, aggregating)
            if value is not None:
                return value
        return None
    if tag == 'not':
        v = truth((yield from evaluate(e[1], env, params, group, aggregating)))
        return None if v is None else not v
    if tag in ('scalar', 'exists', 'in'):
        return (yield from subquery(tag, e, env, params, group, aggregating))
    if tag == 'agg':
        if aggregating:
            raise Fault('NESTED_AGG')
        if group is None:
            raise Fault('NO_GROUP')
        op = e[1]
        values = []
        for member in group:
            values.append((yield from evaluate(e[2], member, params, None, True)))
        nonnull = [v for v in values if v is not None]
        if op == 'count':
            return len(nonnull)
        if op == 'sum':
            if any(type(v) is not int for v in nonnull):
                raise Fault('INT_REQUIRED')
            return sum(nonnull) if nonnull else None
        if op == 'min':
            try:
                return min(nonnull) if nonnull else None
            except TypeError:
                raise Fault('COMPARISON')
        raise Fault('AGG:' + op)
    if tag == 'op':
        op = e[1]
        left = yield from evaluate(e[2], env, params, group, aggregating)
        if op in ('and', 'or'):
            left = truth(left)
            if op == 'and' and left is False:
                return False
            if op == 'or' and left is True:
                return True
            right = truth((yield from evaluate(e[3], env, params, group, aggregating)))
            if op == 'and':
                return False if right is False else (None if left is None or right is None else True)
            return True if right is True else (None if left is None or right is None else False)
        right = yield from evaluate(e[3], env, params, group, aggregating)
        if left is None or right is None:
            return None
        if op == 'eq':
            return left == right
        if op in ('add', 'div'):
            if type(left) is not int or type(right) is not int:
                raise Fault('INT_REQUIRED')
            if op == 'div' and right == 0:
                raise Fault('DIV_ZERO')
            return left + right if op == 'add' else left // right
        if op == 'lt':
            try:
                return left < right
            except TypeError:
                raise Fault('COMPARISON')
        raise Fault('OP:' + op)
    raise Fault('EXPR:' + tag)


def subquery(tag, e, env, params, group, aggregating):
    # IN evaluates its needle even for an empty right side, before the event.
    if tag == 'in':
        needle = yield from evaluate(e[1], env, params, group, aggregating)
        sub = e[2]
    else:
        needle = None
        sub = e[1]
    yield ('trace', ['subquery', tag])
    if tag != 'exists' and len(sub['select']) != 1:
        raise Fault('SUBQUERY_WIDTH')
    # The caller's group and aggregating never cross into the subquery.
    handle = yield ('open', sub, env, tag == 'exists')
    try:
        if tag == 'exists':
            outcome = yield ('next', handle)
            return outcome[0] == 'row'
        if tag == 'scalar':
            outcome = yield ('next', handle)
            if outcome[0] == 'end':
                return None
            first = outcome[1]
            outcome = yield ('next', handle)
            if outcome[0] == 'row':
                raise Fault('SCALAR_MANY')
            return first[0]
        unknown = False
        while True:
            outcome = yield ('next', handle)
            if outcome[0] == 'end':
                return None if unknown else False
            row = outcome[1]
            if needle is None or row[0] is None:
                unknown = True
            elif needle == row[0]:
                return True
    finally:
        yield ('close', handle)
