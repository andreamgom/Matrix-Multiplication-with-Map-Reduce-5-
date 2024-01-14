import time
import numpy as np
import subprocess
from MatrixMultiplication import matrix_multiplication

def run_mapreduce(matrixA_file, matrixB_file, size):

    result = subprocess.run(['python', 'MRMatrixMultiplication.py', '--size=' + str(size), matrixA_file, matrixB_file], capture_output=True, text=True)


    output = result.stdout

    return output

def benchmark():
    sizes = [50, 100, 250, 500, 750, 1024]

    for n in sizes:

        A = np.random.rand(n, n)
        B = np.random.rand(n, n)

        start = time.time()
        matrix_multiplication(A, B)
        end = time.time()
        print(f"n = {n}: Execution time for normal matrix multiplication = {end - start:.6f} seconds")

        # Guarda las matrices en archivos de texto
        with open('matrixA.txt', 'w') as f:
            for i in range(n):
                for j in range(n):
                    f.write(f"A {i} {j} {A[i][j]}\n")

        with open('matrixB.txt', 'w') as f:
            for i in range(n):
                for j in range(n):
                    f.write(f"B {i} {j} {B[i][j]}\n")

        start = time.time()
        run_mapreduce('matrixA.txt', 'matrixB.txt', n)
        end = time.time()
        print(f"n = {n}: Execution time for MapReduce matrix multiplication = {end - start:.6f} seconds")

if __name__ == '__main__':
    benchmark()
