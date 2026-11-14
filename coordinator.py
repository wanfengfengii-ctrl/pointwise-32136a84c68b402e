"""Coordinator: owns live generators, opaque handles, request routing,
business-fault injection and trace collection.

It drives the top-level relation generator, answers ("eval", ...) requests by
running exprkernel.evaluate generators, and answers ("open"/"next"/"close", ...)
requests by managing relationkernel.execute generators behind handles. It
reads only the top-level select output names; it never interprets query keys,
expression tags, operators or row values, and keeps no interpreter of its own.
Both kernels are reached through their module attributes so either can be
replaced without changing the routing.
"""
import exprkernel
import relationkernel
from protocol import Fault

_START = object()  # marks "not started yet" for a paused generator


def run(query, tables, params=None):
    params = {} if params is None else params
    trace = []
    rows = []
    handles = {}   # handle -> [generator, pending reply, pending fault]
    parents = {}   # handle -> owning handle (None for top-level opens)
    pull_stack = []

    def close_handle(handle):
        state = handles.pop(handle, None)
        parents.pop(handle, None)
        if state is not None and state[0] is not None:
            state[0].close()
            state[0] = None
        # Early termination and faults must not leave unfinished descendants.
        for other in list(handles):
            parent = parents.get(other)
            while parent is not None and parent is not handle:
                parent = parents.get(parent)
            if parent is handle:
                close_handle(other)

    def open_handle(sub, outer, existence):
        # Only establish the relation generator; never advance it here.
        gen = relationkernel.execute(sub, outer, tables, existence)
        handle = object()
        handles[handle] = [gen, _START, None]
        parents[handle] = pull_stack[-1] if pull_stack else None
        return handle

    def pull_relation(state, owner=None):
        """Advance a relation generator to its next final row or to the end."""
        pull_stack.append(owner)
        try:
            gen = state[0]
            while True:
                try:
                    if state[2] is not None:
                        fault = state[2]
                        state[2] = None
                        request = gen.throw(fault)
                    elif state[1] is _START:
                        request = next(gen)
                    else:
                        request = gen.send(state[1])
                except StopIteration:
                    state[0] = None
                    return ('end',)
                kind = request[0]
                if kind == 'row':
                    state[1] = None
                    return ('row', request[1])
                if kind == 'trace':
                    trace.append(request[1])
                    state[1] = None
                else:
                    try:
                        state[1] = run_expression(exprkernel.evaluate(
                            request[1], request[2], params, request[3], request[4]))
                    except Fault as fault:
                        state[2] = fault
        finally:
            pull_stack.pop()

    def run_expression(gen):
        """Drive an expression generator to its scalar or to a business fault."""
        reply = _START
        pending = None
        while True:
            try:
                if pending is not None:
                    request = gen.throw(pending)
                    pending = None
                elif reply is _START:
                    request = next(gen)
                else:
                    request = gen.send(reply)
            except StopIteration as stop:
                return stop.value
            kind = request[0]
            if kind == 'trace':
                trace.append(request[1])
                reply = None
            elif kind == 'open':
                reply = open_handle(request[1], request[2], request[3])
            elif kind == 'next':
                try:
                    reply = pull_relation(handles[request[1]], request[1])
                except Fault as fault:
                    pending = fault
            else:
                close_handle(request[1])
                reply = None

    top = [relationkernel.execute(query, [], tables), _START, None]
    try:
        while True:
            outcome = pull_relation(top)
            if outcome[0] == 'end':
                break
            rows.append(outcome[1])
    except Fault as exc:
        for handle in list(handles):
            close_handle(handle)
        return {'error': str(exc), 'trace': trace}
    return {'columns': [pair[0] for pair in query['select']], 'rows': rows, 'trace': trace}
