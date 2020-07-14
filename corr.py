import numpy as np
import pandas as pd
import ast
import itertools
from functools import reduce
import math

# used during testing
A = [0.38,0.87,0.09,0.32,0.92,0.04,0.31, 0.70, 0.67, 0.80]
B = [0.34,0.67,0.01,0.22,0.52,0.64,0.81, 0.30, 0.67, 0.90]
C = [0.38,0.27,7.09,0.39,0.72,0.04,4.31, 1.70, 0.67, 0.10]
D = [0.48,0.47,5.09,1.32,0.62,0.54,0.81, 2.70, 0.47, 0.30]
#
#Total correlation
def entropy(group):
    arr = []
    for g in group:
        arr.append(ast.literal_eval(g[1]))
    arrs = np.array(arr)
    entrp = []
    for arr in arrs:
        probs = [np.mean(arr == c) for c in set(arr)]
        entrp.append(sum(-p * np.log2(p) for p in probs))
    return entrp


def joint_entropy(group):
    arr = []
    for g in group:
        arr.append(ast.literal_eval(g[1]))
    X = np.array(arr)
    return  sum(-p * np.log2(p) if p > 0 else 0
        for p in (np.mean(reduce(np.logical_and, (predictions == c for predictions, c in zip(X, classes))))
        for classes in itertools.product(*[set(x) for x in X])))


def computeTC(group):
    H = entropy(group)
    H_joint = joint_entropy(group)
    T = sum(H) - H_joint
    return T



#aggeragate vectors for person correlation
def agg_vec(group, method):
    arr = []
    for g in group:
        arr.append(ast.literal_eval(g[1]))
    vec = np.array(arr)
    #min
    if method==0:
        return list(vec.min(0))
    #max
    elif method==1:
        return list(vec.max(0))
    #avg
    elif method==2:
        return list(np.average(vec, axis=0))
    #weighted sum
    elif method==3:
        cols = vec.T
        s_col = sum(vec,0)
        w = np.array([[cols[i][0]/s_col[i], cols[i][1]/s_col[i]] for i in range(len(cols))]).T
        res = sum(w*vec,0)
        return (list(res))



#pearson correlation
def computePC(x, y):
    n = len(x)
    sum_x = float(sum(x))
    sum_y = float(sum(y))
    sum_x_sq = sum(map(lambda x: pow(x, 2), x))
    sum_y_sq = sum(map(lambda x: pow(x, 2), y))
    psum = sum(list(map(lambda x, y: x * y, x, y)))
    num = psum - (sum_x * sum_y/n)
    den = pow((sum_x_sq - pow(sum_x, 2) / n) * (sum_y_sq - pow(sum_y, 2) / n), 0.5)
    if den == 0: return 0
    return num / den

def computeSpear(x,y):
    xranks = pd.Series(x).rank()
    yranks = pd.Series(y).rank()
    coef = computePC(xranks,yranks)
    return coef

def computeKendall(x,y):
    concordant = 0
    discordant = 0
    xt = 0
    yt = 0
    n = len(x)
    j = 0
    while j < len(x) :
        i = 0
        while i < j :
            if x[i] > x[j] and y[i] > y[j]:
                concordant = concordant + 1
            if x[i] < x[j] and y[i] < y[j]:
                concordant = concordant + 1
            if x[i] > x[j] and y[i] < y[j]:
                discordant = discordant + 1
            if x[i] < x[j] and y[i] > y[j]:
                discordant = discordant + 1
            i = i + 1
            if x[i] == x[j] and y[i] != y[j]:
                xt = xt + 1
            if y[i] == y[j] and x[i] != x[j]:
                yt = yt + 1

        j = j + 1
    tau2 = (concordant - discordant)/ ((math.sqrt(discordant + concordant + xt)) * (math.sqrt(discordant + concordant + yt)))
    return tau2