"""Relation kernel: table scans, lateral source enumeration, outer-join null
filling, grouping, projection order, distinct, ordering and limit.

execute(q, outer, tables, existence=False) returns a generator. Start it with
next(), answer its requests with send(), and inject business failures with
throw(protocol.Fault(message)). It never evaluates anything itself; every
expression reaches the coordinator as an ("eval", e, env, group, aggregating)
request and the replied scalar is used verbatim. Boolean checking and
filtering of where/having replies happens here. Emitted messages:

* ("eval", e, env, group, aggregating) -> respond the expression scalar
* ("trace", event)                     -> respond None
* ("row", row)                         -> deliver one final result row

Internal pipelines forward eval/trace requests upward unchanged; only final
rows of this query are delivered as ("row", row), never lateral source rows.
"""
import json
from protocol import Fault

_START = object()  # marks "not started yet" in the manual drive loops


def truth(value):
    if value is None:
        return None
    if type(value) is not bool:
        raise Fault('BOOL_REQUIRED')
    return value


def key(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def execute(q, outer, tables, existence=False):
    yield from body(q, outer, tables, existence)


def body(q, outer, tables, existence):
    if q.get('limit') == 0:
        return
    stream = projected(q, outer, tables, existence)
    if not existence and q.get('distinct', False):
        stream = unique(stream)
    if not existence and q.get('order'):
        stream = sorted_rows(stream, q['order'])
    try:
        limit = q.get('limit')
        count = 0
        reply = _START
        while limit is None or count < limit:
            try:
                msg = next(stream) if reply is _START else stream.send(reply)
            except StopIteration:
                return
            if msg[0] == 'row':
                reply = yield ('row', msg[1])
                count += 1
            else:
                reply = yield msg
    finally:
        stream.close()


def projected(q, outer, tables, existence):
    cand = candidates(q, outer, tables)
    reply = _START
    try:
        while True:
            try:
                msg = next(cand) if reply is _START else cand.send(reply)
            except StopIteration:
                return
            if msg[0] == 'val':
                env, group = msg[1]
                row = []
                if not existence:
                    for _name, e in q['select']:
                        row.append((yield ('eval', e, env, group, False)))
                yield ('row', row)
                reply = None
            else:
                reply = yield msg
    finally:
        cand.close()


def candidates(q, outer, tables):
    src = source(q, outer, tables, 0, [])
    try:
        if 'group' not in q:
            reply = _START
            while True:
                try:
                    msg = next(src) if reply is _START else src.send(reply)
                except StopIteration:
                    return
                if msg[0] == 'val':
                    yield ('val', (msg[1], None))
                    reply = None
                else:
                    reply = yield msg
        groups = {}
        reply = _START
        while True:
            try:
                msg = next(src) if reply is _START else src.send(reply)
            except StopIteration:
                break
            if msg[0] == 'val':
                env = msg[1]
                values = []
                for e in q['group']:
                    values.append((yield ('eval', e, env, None, False)))
                groups.setdefault(key(values), []).append(env)
                reply = None
            else:
                reply = yield msg
        if not q['group'] and not groups:
            groups['[]'] = []
        for members in groups.values():
            if members:
                env = members[0]
            else:
                # No physical source is scanned here; only declared layouts.
                frame = []
                for item in q.get('from', []):
                    if 'table' in item:
                        name = item['table']
                        if name not in tables:
                            raise Fault('TABLE:' + name)
                        names = tables[name]['columns']
                    else:
                        names = [pair[0] for pair in item['query']['select']]
                    frame.append((item['as'], names, [None] * len(names)))
                env = [frame] + outer
            having = q.get('having', ['lit', True])
            if truth((yield ('eval', having, env, members, False))) is True:
                yield ('val', (env, members))
    finally:
        src.close()


def source(q, outer, tables, index, frame):
    if index == len(q.get('from', [])):
        env = [frame] + outer
        where = q.get('where', ['lit', True])
        if truth((yield ('eval', where, env, None, False))) is True:
            yield ('val', env)
        return
    item = q['from'][index]
    if 'table' in item:
        name = item['table']
        yield ('trace', ['scan', name])
        if name not in tables:
            raise Fault('TABLE:' + name)
        names = tables[name]['columns']
        seen = False
        for row in tables[name]['rows']:
            seen = True
            binding = (item['as'], names, row)
            yield from source(q, outer, tables, index + 1, frame + [binding])
    else:
        names = [pair[0] for pair in item['query']['select']]
        # A lateral relation sees the current prefix, including an empty frame.
        pump = lateral(execute(item['query'], [frame] + outer, tables))
        reply = _START
        seen = False
        try:
            while True:
                try:
                    msg = next(pump) if reply is _START else pump.send(reply)
                except StopIteration:
                    break
                if msg[0] == 'val':
                    seen = True
                    binding = (item['as'], names, msg[1])
                    yield from source(q, outer, tables, index + 1, frame + [binding])
                    reply = None
                else:
                    reply = yield msg
        finally:
            pump.close()
    if not seen and item.get('left', False):
        binding = (item['as'], names, [None] * len(names))
        yield from source(q, outer, tables, index + 1, frame + [binding])


def lateral(child):
    """Drive a nested execute() generator: forward its eval/trace requests
    upward, convert its final rows into ('val', row) source rows."""
    try:
        reply = _START
        while True:
            try:
                msg = next(child) if reply is _START else child.send(reply)
            except StopIteration:
                return
            if msg[0] == 'row':
                yield ('val', msg[1])
                reply = None
            else:
                reply = yield msg
    finally:
        child.close()


def unique(stream):
    seen = set()
    reply = _START
    try:
        while True:
            try:
                msg = next(stream) if reply is _START else stream.send(reply)
            except StopIteration:
                return
            if msg[0] == 'row':
                encoded = key(msg[1])
                if encoded in seen:
                    reply = None
                else:
                    seen.add(encoded)
                    reply = yield msg
            else:
                reply = yield msg
    finally:
        stream.close()


def sorted_rows(stream, order):
    rows = []
    reply = _START
    try:
        while True:
            try:
                msg = next(stream) if reply is _START else stream.send(reply)
            except StopIteration:
                break
            if msg[0] == 'row':
                rows.append(msg[1])
                reply = None
            else:
                reply = yield msg
    finally:
        stream.close()
    for index, descending in reversed(order):
        def sort_key(row):
            v = row[index]
            return (v is not None, type(v).__name__, v)
        rows.sort(key=sort_key, reverse=descending)
    for row in rows:
        yield ('row', row)
