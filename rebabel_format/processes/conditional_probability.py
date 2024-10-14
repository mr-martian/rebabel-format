from rebabel_format.process import Process
from rebabel_format.parameters import Parameter, QueryParameter
from rebabel_format.query import ResultTable

from collections import Counter, defaultdict
from itertools import combinations

class ConditionalProbability(Process):
    name = 'conditional_probability'

    query = QueryParameter()
    center = Parameter(type=str, default='Center')
    target_feature = Parameter(type=str)
    features = Parameter(type=list)
    max_combinations = Parameter(type=int, default=2)

    def run(self):
        table = ResultTable(self.db, self.query)
        table.add_features(self.center, [self.target_feature] + self.features)

        result_count = 0
        target_count = 0
        results = defaultdict(lambda: defaultdict(Counter))
        for nodes, features in table.results():
            result_count += 1
            dct = features.get(nodes.get(self.center), {})
            if self.target_feature not in dct:
                continue
            target_count += 1
            names = [k for k in dct if k in self.features]
            for i in range(self.max_combinations):
                for keys in combinations(names, i+1):
                    values = tuple([dct[k] for k in keys])
                    results[keys][values][dct[self.target_feature]] += 1

        for condition in sorted(results.keys(), key=lambda x: (len(x), x)):
            print(f'Conditioning on {", ".join(condition)}:')
            for values, probs in sorted(results[condition].items()):
                pieces = [f'{k}={repr(v)}' for k, v in zip(condition, values)]
                given = ', '.join(pieces)
                total = sum(x[1] for x in probs.most_common())
                print(f'\tP({self.target_feature} = _ | {given})')
                for val, count in probs.most_common():
                    print(f'\t\t{repr(val)} => {count} / {total} = {100.0*count/total:.4}%')
            print()

        print(f'Query had {result_count} results, {target_count} ({100.0*target_count/result_count:.4}%) of which contained the target feature.')
