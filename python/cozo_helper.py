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
        ret = {'pred': self._pred_name, 'args': list(args)}
        return ret


Gt = PredicateClass('Gt')
Lt = PredicateClass('Lt')
Ge = PredicateClass('Ge')
Le = PredicateClass('Le')
Eq = PredicateClass('Eq')
Neq = PredicateClass('Neq')


def Const(item):
    return {'const': item}


__all__ = ['Gt', 'Lt', 'Ge', 'Le', 'Eq', 'Neq', 'Q', 'T', 'R', 'Const']

if __name__ == '__main__':
    import json

    rules = [
        R.ancestor(["?a", "?b"],
                   T.parent("?a", "?b")),
        R.ancestor(["?a", "?b"],
                   T.parent("?a", "?c"),
                   R.ancestor("?c", "?b")),
        Q(["?a"],
          R.ancestor("?a", {"name": "Anne"}))
    ]
    print(json.dumps(rules, indent=2))
    rules = [
        Q.at("1990-01-01")(["?old_than_anne"],
                           T.age({"name": "Anne"}, "?anne_age"),
                           T.age("?older_than_anne", "?age"),
                           Gt("?age", "?anne_age"))
    ]
    print(json.dumps(rules, indent=2))
