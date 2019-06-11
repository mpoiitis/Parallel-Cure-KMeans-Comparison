### 1st experiment  
Number of intermediate clusters: 10
Number of clusters: 5

1) KMeans: 55744 ms (in whole dataset)
  
2a) Agglomerative Hierarchical for Post-Processing: 18.96 ms  
    Silhouette Coefficient = 0.329  
    Num of clusters = 5

2b) SHAS for Post-Processing: 2224 ms  
    Silhouette Coefficient = 0.3627961989983045  
    Num of clusters = 5  

=====================================================  
  
Sample size: 0.5% - 28167 points  
    
3a) SHAS for Pre-Processing: 615964 ms
    Num of clusters = 10
3b) Agglomerative Hierarchical for Pre-Processing: 118935 ms
    Num of clusters = 10
    
4) CURE: 8347 ms  

### 2nd experiment  

Number of intermediate clusters: 100
Number of clusters: 5

1) KMeans: 118725 ms (in whole dataset)
  
2a) Agglomerative Hierarchical for Post-Processing: 28.008 ms  
    Silhouette Coefficient = 0.437  
    Num of clusters = 5

2b) SHAS for Post-Processing: 2861 ms  
    Silhouette Coefficient = 0.35417609259322574  
    Num of clusters = 5  

=====================================================  
  
Sample size: 0.01% - 5585 points  
    
3a) SHAS for Pre-Processing: 4569 ms
    Num of clusters = 100
3b) Agglomerative Hierarchical for Pre-Processing: 2212.05735206604 ms  
    Silhouette Coefficient = 0.311 (intermediate)  
    Num of clusters = 100  
    
4) CURE: TBD  

### 3rd experiment  

Number of intermediate clusters: 10
Number of clusters: 5

1) KMeans: 41747 ms (in whole dataset)
  
2) Agglomerative Hierarchical for Post-Processing: 7.978916168212891 ms  
    Silhouette Coefficient = 0.329  
    Num of clusters = 5  

=====================================================  
  
Sample size: 0.01% - 5585 points  
    
3) Agglomerative Hierarchical for Pre-Processing: 2186.1536502838135 ms
    Silhouette Coefficient = 0.314 (intermediate step)  
    Num of clusters = 10
    
4) CURE: 7965 ms  
    Silhouette Coefficient = 0.6259368183047109  
    
### 4th experiment  

Number of intermediate clusters: 10
Number of clusters: 5

1) KMeans: 46053 ms (in whole dataset)
  
2) SHAS for Post-Processing: 2372 ms  
    Silhouette Coefficient = 0.3627961989983045  
    Num of clusters = 5  

=====================================================  
  
Sample size: 0.01% - 5585 points  
    
3b) SHAS for Pre-Processing: 67765 ms
    Silhouette Coefficient ~ 0.345 (intermediate step)  
    Num of clusters = 10
    
4) CURE: 7233 ms  
    Silhouette Coefficient = 0.3239711165337978  
    
### 5th experiment  

Number of intermediate clusters: 100
Number of clusters: 5

1) KMeans: 106329 ms (in whole dataset)
  
2) Agglomerative Hierarchical for Post-Processing: 7.006406784057617 ms  
    Silhouette Coefficient = 0.437  
    Num of clusters = 5  

=====================================================  
  
Sample size: 0.1% - ~56000 points  
    
3) Agglomerative Hierarchical for Pre-Processing: Memory Error  

### 6th experiment  

Number of intermediate clusters: 100
Number of clusters: 5

1) KMeans: 122082 ms (in whole dataset)
  
2) SHAS for Post-Processing: 2602 ms  
    Silhouette Coefficient = 0.35417609259322574  
    Num of clusters = 5  

=====================================================  
  
Sample size: 0.01% - 5550 points  
    
3) SHAS for Pre-Processing: 153658 ms  
    Num of clusters = 100  
    
4) CURE: TBD