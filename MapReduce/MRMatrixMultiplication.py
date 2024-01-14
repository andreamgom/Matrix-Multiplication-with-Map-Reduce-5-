from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMatrixMultiplication(MRJob):
    def configure_args(self):
        super(MRMatrixMultiplication, self).configure_args()
        self.add_passthru_arg('--size', type=int, help='Size of the matrices')

    def mapper(self, _, line):
        matrix, i, j, value = line.split()
        i = int(i)
        j = int(j)
        if matrix == 'A':
            for k in range(self.options.size):
                yield (i, k), ('A', j, value)
        else:
            for k in range(self.options.size):
                yield (k, j), ('B', i, value)

    def reducer(self, key, values):
        i, j = map(int, key)  # convert keys to integers
        A = [0] * self.options.size
        B = [0] * self.options.size
        for matrix, index, value in values:
            index = int(index)
            value = float(value)
            if matrix == 'A':
                A[index] = value
            else:
                B[index] = value
        yield key, sum(a * b for a, b in zip(A, B))

if __name__ == '__main__':
    MRMatrixMultiplication.run()


#python MRMatrixMultiplication.py --size=2 matrix_values.txt
