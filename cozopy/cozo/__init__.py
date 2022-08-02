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
    keyword = 'keyword'
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


def Attribute(keyword, typing, id, cardinality, index, history):
    ret = {
        'keyword': keyword,
        'type': typing,
        'cardinality': cardinality,
        'index': index,
        'history': history
    }
    if id is not None:
        ret['id'] = id
    return ret


def PutAttr(keyword, typing, id=None, cardinality=Cardinality.one, index=Indexing.none, history=False):
    return {
        'put': Attribute(keyword, typing, id, cardinality, index, history)
    }


def RetractAttr(keyword, typing, id, cardinality, index, history):
    return {
        'retract': Attribute(keyword, typing, id, cardinality, index, history)
    }


def Put(d):
    return {'put': d}


def Retract(d):
    return {'retract': d}


def Pull(variable, spec):
    return {'pull': variable, 'spec': spec}


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


class PredicateClass:
    def __init__(self, pred_name):
        self._pred_name = pred_name

    def __getattr__(self, name):
        if self._pred_name is None:
            return self.__class__(name)
        else:
            raise Exception("cannot nest predicate name")

    def __call__(self, *args):
        if self._pred_name is None:
            raise Exception("you need to set the predicate name first")
        ret = {'op': self._pred_name, 'args': list(args)}
        return ret


Gt = PredicateClass('Gt')
Lt = PredicateClass('Lt')
Ge = PredicateClass('Ge')
Le = PredicateClass('Le')
Eq = PredicateClass('Eq')
Neq = PredicateClass('Neq')
Add = PredicateClass('Add')
Sub = PredicateClass('Sub')
Mul = PredicateClass('Mul')
Div = PredicateClass('Div')
StrCat = PredicateClass('StrCat')


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
           'Retract', 'Pull', 'StrCat', 'Unify']
