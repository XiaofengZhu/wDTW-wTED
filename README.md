# wDTW-wTED
Code used in Semantic Document Distance Measures and
Unsupervised Document Revision Detection (accepted by IJCNLP 2017)

All the distance measures are under src/main/java/com/spark/cluster
Please adjust the spark setting file pom.xml accordingly. 

To replicate the revision detection results, use the normalization function instead of the default distance measures. Simulated data sets are under simulated_data_sets. Wikipedia revision dumps can be found on https://snap.stanford.edu/data/wiki-meta.html.

This project is licensed under the terms of the Apache license. Kindly cite our paper (https://arxiv.org/abs/1709.01256) when you use our code.

@inproceedings{zhu-etal-2017-semantic,
    title = "Semantic Document Distance Measures and Unsupervised Document Revision Detection",
    author = "Zhu, Xiaofeng  and
      Klabjan, Diego  and
      Bless, Patrick",
    booktitle = "Proceedings of the Eighth International Joint Conference on Natural Language Processing (Volume 1: Long Papers)",
    month = nov,
    year = "2017",
    address = "Taipei, Taiwan",
    publisher = "Asian Federation of Natural Language Processing",
    url = "https://www.aclweb.org/anthology/I17-1095",
    pages = "947--956",
    abstract = "In this paper, we model the document revision detection problem as a minimum cost branching problem that relies on computing document distances. Furthermore, we propose two new document distance measures, word vector-based Dynamic Time Warping (wDTW) and word vector-based Tree Edit Distance (wTED). Our revision detection system is designed for a large scale corpus and implemented in Apache Spark. We demonstrate that our system can more precisely detect revisions than state-of-the-art methods by utilizing the Wikipedia revision dumps and simulated data sets.",
}
