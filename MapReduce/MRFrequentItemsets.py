from mrjob.job import MRJob
from mrjob.step import MRStep
import itertools

class MRFrequentItemsets(MRJob):
    def configure_args(self):
        super(MRFrequentItemsets, self).configure_args()
        self.add_passthru_arg('--support', type=int, help='Minimum support count')

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.second_reducer)
        ]

    def mapper(self, _, line):
        items = sorted([int(x) for x in line.split()])
        for itemset in itertools.chain(*[itertools.combinations(items, i + 1) for i in range(len(items))]):
            yield itemset, 1

    def reducer(self, key, values):
        total = sum(values)
        if total >= self.options.support:
            yield None, (total, key)

    def second_reducer(self, _, values):
        yield max(values)

if __name__ == '__main__':
    MRFrequentItemsets.run()

# python MRFrequentItemsets.py --support=2 input_transaction.txt

