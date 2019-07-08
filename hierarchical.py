from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
import numpy as np
import time
import os

def clusters(fileName, numClusters=5):
    dataFile = open(fileName, "r")
    X = list()
    for line in dataFile.readlines():
        line = line[1:-2]
        coords = line.split(",")
        coords = [float(coord) for coord in coords]
        X.append(coords)


    X = np.asarray(X)

    start = time.time()
    cluster = AgglomerativeClustering(n_clusters=numClusters, affinity='euclidean', linkage='ward')
    predicted = cluster.fit_predict(X)

    print("Silhouette Coefficient: %0.3f" % silhouette_score(X, predicted))
    end = time.time()
    print("Time needed: " + str((end - start)*1000) + " ms")

    return X, predicted

def writeListToFile(data, predictions, filename):
    with open(filename, mode='w', encoding='utf-8') as myfile:
        for record in zip(data, predictions):
            curData = record[0]
            curData = [str(el) for el in curData]
            curPrediction = record[1]
            myfile.write(" ".join(curData) + " " + str(curPrediction) + "\n")


clusters(os.path.abspath("C:/Users/Marinos/IdeaProjects/CURE-algorithm/src/main/python/pythonData/kMeansClusters"))

data, predictions = clusters(os.path.abspath("C:/Users/Marinos/IdeaProjects/CURE-algorithm/src/main/python/pythonData/sample"), 10)
writeListToFile(data, predictions, os.path.abspath('C:/Users/Marinos/IdeaProjects/CURE-algorithm/src/main/python/pythonData/intermediateClusters.txt'))

