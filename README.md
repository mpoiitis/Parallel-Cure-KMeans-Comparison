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
  
Sample size: 0.01% - 28167 points  
    
3a) SHAS for Pre-Processing: 4569 ms
    Num of clusters = 100
3b) Agglomerative Hierarchical for Pre-Processing: 209.47 ms
    Num of clusters = 100
    
4) CURE: TBD