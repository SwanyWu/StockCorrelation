# StockCorrelation
Pyspark inplementation of correlation calculation
Data: each stock price is a vector of length 30.
Corr: correlation function takes two price vectors as input, when num(input vectors)>2, the aggeregation function will reduce them to two.
MapReduce: the 'key' is the threshold for correlations. For Pearson it's 0.85 and 7.5 for total correlation. 
