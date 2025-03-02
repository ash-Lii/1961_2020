import numpy as np
from scipy.stats import norm

outPath = "./data-analysis.csv"
filePath = "./dataprocessed.csv"
# num,wd,jd,year,ave,max,min,rise,drop

def mann_kendall_test(data):
    n = len(data)
    if n < 2:
        raise ValueError("Time series must have at least two data points.")
    
    # 计算S
    s = 0
    for k in range(n - 1):
        for j in range(k + 1, n):
            s += np.sign(data[j] - data[k])
    
    # 计算方差
    unique_data = np.unique(data)
    g = len(unique_data)
    tp = np.array([np.sum(data == unique_data[i]) for i in range(g)])
    var_s = (n * (n - 1) * (2 * n + 5) - np.sum(tp * (tp - 1) * (2 * tp + 5))) / 18
    
    # 计算标准化统计量Z
    if s > 0:
        z = (s - 1) / np.sqrt(var_s)
    elif s < 0:
        z = (s + 1) / np.sqrt(var_s)
    else:
        z = 0
    
    # 计算P值
    p = 2 * (1 - norm.cdf(abs(z)))
    
    return s, z, p



# 读取数据
data = np.genfromtxt(filePath, delimiter=',', skip_header=1)

# 获取站点编号
nums = np.unique(data[:, 0])


f = open(outPath, "w")
f.write("num,wd,jd,ave,max,min,rise,drop" + "\n")
for num in nums:

    # 获取指定站点所有数据
    d = data[data[:, 0] == num].T
    #用于储存各个Z值
    trend = np.zeros(9)
    # 用于存储显著突变点
    sig_change_points = np.zeros((9, 50))

    for i in range(4, 9):
        s,z,p = mann_kendall_test(d[i])
        trend[i] = round(z, 2)


        

    f.write(str(int(d[0][0]+50000)))
    f.write(',')
    f.write(str(int(d[1][0])))
    f.write(',')
    f.write(str(int(d[2][0])))
    f.write(',')
    f.write(str(trend[4]))
    f.write(',')
    f.write(str(trend[5]))
    f.write(',')
    f.write(str(trend[6]))
    f.write(',')
    f.write(str(trend[7]))
    f.write(',')
    f.write(str(trend[8]))
    f.write('\n')
            
