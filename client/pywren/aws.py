from __future__ import print_function

from cvxpy import *

import pywren
import time
import numpy as np
import random

from scipy.ndimage.filters import convolve as convolveim


def _exec(f, args):
    start = time.time()
    wrenexec = pywren.default_executor()
    fut = wrenexec.call_async(f, args)
    print(fut.callset_id)

    res = fut.result()
    print(res)
    print("time: ", time.time() - start)


def simple_add():
    def simple_add(a):
        return a + 1

    print("start simple add..")
    _exec(simple_add, 0)


def compute_flops():
    def compute_flops(loopcount):
        a = np.random.rand(*(100, 100, 100))
        b = np.random.rand(*(10, 10, 10))

        conv1 = convolveim(a, b, mode='constant')
        # conv2 = convolvesig(a,b, mode = 'same')
        return conv1  # FLOPS / (t2-t1)

    print("start flops..")
    _exec(compute_flops, 0)


def trivial_cvx():
    def trivial(x):
        # Create two scalar optimization variables.
        x = Variable()
        y = Variable()

        # Create two constraints.
        constraints = [x + y == 1,
                       x - y >= 1]

        # Form objective.
        obj = Minimize(square(x - y))

        # Form and solve problem.
        prob = Problem(obj, constraints)
        prob.solve()  # Returns the optimal value.
        # print("status:", prob.status)
        # print("optimal value", prob.value)
        # print("optimal var", x.value, y.value)
        return prob.value

    print("start cvx..")
    _exec(trivial, 0)


if __name__ == "__main__":
    for _ in range(20):
        trivial_cvx()
        # compute_flops()
        # simple_add()
