import json
from enum import Enum

from cozopy import CozoDbPy


class CozoDb:
    def __init__(self, *args, **kwargs):
        self.inner = CozoDbPy(*args, **kwargs)

    def tx_attr(self, payload):
        return json.loads(self.inner.transact_attributes(json.dumps({'attrs': payload}, ensure_ascii=False)))

    def tx(self, payload):
        return json.loads(self.inner.transact_triples(json.dumps({'tx': payload}, ensure_ascii=False)))

    def run(self, q, out=None):
        payload = {'q': q}
        if out is not None:
            payload['out'] = out
        return json.loads(self.inner.run_query(json.dumps(payload, ensure_ascii=False)))


class Typing(str, Enum):
    ref = 'ref'
    component = 'component'
    bool = 'bool'
    int = 'int'
    float = 'float'
    string = 'string'
    name = 'name'
    uuid = 'uuid'
    timestamp = 'timestamp'
    bytes = 'bytes'
    list = 'list'


class Cardinality(str, Enum):
    one = 'one'
    many = 'many'


class Indexing(str, Enum):
    none = 'none'
    indexed = 'indexed'
    unique = 'unique'
    identity = 'identity'


def Attribute(name, typing, id, cardinality, index, history):
    ret = {
        'name': name,
        'type': typing,
        'cardinality': cardinality,
        'index': index,
        'history': history
    }
    if id is not None:
        ret['id'] = id
    return ret


def PutAttr(name, typing, id=None, cardinality=Cardinality.one, index=Indexing.none, history=False):
    return {
        'put': Attribute(name, typing, id, cardinality, index, history)
    }


def RetractAttr(name, typing, id, cardinality, index, history):
    return {
        'retract': Attribute(name, typing, id, cardinality, index, history)
    }


def Put(d):
    return {'put': d}


def Retract(d):
    return {'retract': d}


def Pull(variable, spec):
    return {'pull': variable, 'spec': spec}


class DefAttrs:
    def __init__(self, prefix):
        self.prefix = prefix
        self.attrs = []

    def __call__(self, *args, **kwargs):
        return self.attrs

    def __getattr__(self, item):
        return DefAttributesHelper(self, item)


class DefAttributesHelper:
    def __init__(self, parent, name):
        self.parent = parent
        self.name = name

    def __call__(self, typing, id=None, cardinality=Cardinality.one, index=Indexing.none, history=False):
        name = f'{self.parent.prefix}.{self.name}'
        self.parent.attrs.append({
            'put': Attribute(name, typing, id, cardinality, index, history)
        })
        return self.parent


class TripleClass:
    def __init__(self, attr_name):
        self._attr_name = attr_name

    def __getattr__(self, name):
        if self._attr_name is None:
            return self.__class__(name)
        else:
            return self.__class__(self._attr_name + '.' + name)

    def __call__(self, entity, value):
        if self._attr_name is None:
            raise Exception("you need to set the triple attribute first")
        return [entity, self._attr_name, value]


T = TripleClass(None)


class RuleClass:
    def __init__(self, rule_name):
        self._rule_name = rule_name
        self._at = None

    def __getattr__(self, name):
        if self._rule_name is None:
            return self.__class__(name)
        elif name == 'at':
            def closure(time):
                self._at = time
                return self

            return closure
        else:
            raise Exception("cannot nest rule name")

    def __call__(self, *args):
        if self._rule_name is None:
            raise Exception("you need to set the rule name first")
        ret = {'rule': self._rule_name, 'args': list(args)}
        if self._at:
            ret['at'] = self._at
        return ret


R = RuleClass(None)
Q = RuleClass('?')


class OpClass:
    def __init__(self, op_name):
        self._op_name = op_name

    def __getattr__(self, name):
        if self._op_name is None:
            return self.__class__(name)
        else:
            raise Exception("cannot nest op name")

    def __call__(self, *args):
        if self._op_name is None:
            raise Exception("you need to set the op name first")
        ret = {'op': self._op_name, 'args': list(args)}
        return ret


Gt = OpClass('Gt')
Lt = OpClass('Lt')
Ge = OpClass('Ge')
Le = OpClass('Le')
Eq = OpClass('Eq')
Neq = OpClass('Neq')
Add = OpClass('Add')
Sub = OpClass('Sub')
Mul = OpClass('Mul')
Div = OpClass('Div')
StrCat = OpClass('StrCat')


class AggrClass:
    def __init__(self, aggr_name):
        self._aggr_name = aggr_name

    def __getattr__(self, name):
        if self._aggr_name is None:
            return self.__class__(name)
        else:
            raise Exception("cannot nest aggr name")

    def __call__(self, symb):
        if self._aggr_name is None:
            raise Exception("you need to set the predicate name first")
        ret = {'aggr': self._aggr_name, 'symb': symb}
        return ret


Count = AggrClass('Count')
Min = AggrClass('Min')
Max = AggrClass('Max')


def Const(item):
    return {'const': item}


def Conj(*items):
    return {'conj': items}


def Disj(*items):
    return {'disj': items}


def NotExists(item):
    return {'not_exists': item}


def Unify(binding, expr):
    return {'unify': binding, 'expr': expr}


__all__ = ['Gt', 'Lt', 'Ge', 'Le', 'Eq', 'Neq', 'Add', 'Sub', 'Mul', 'Div', 'Q', 'T', 'R', 'Const', 'Conj', 'Disj',
           'NotExists', 'CozoDb', 'Typing', 'Cardinality', 'Indexing', 'PutAttr', 'RetractAttr', 'Attribute', 'Put',
           'Retract', 'Pull', 'StrCat', 'Unify', 'DefAttrs', 'Count', 'Min', 'Max']
