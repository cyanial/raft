# Raft Impl

## Raft 实现细节

To-do: xxx

## To-dos:

0. Agree Faster [Issues#3](/../../issues/3)
1. 多跑几次测试
2. 优化 ugly if-else
 
## 测试结果

### 2A

 ```sh
Test (2A): initial election ...
  ... Passed --   3.1  3   60   17666    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  134   27867    0
Test (2A): multiple elections ...
  ... Passed --   5.5  7  672  139202    0
PASS
ok  	github.com/cyanial/raft	13.800s

real	0m14.123s
user	0m1.611s
sys     0m0.716s
 ```

### 2B

```bash
Test (2B): basic agreement ...
  ... Passed --   0.8  3   16    4612    3
Test (2B): RPC byte count ...
  ... Passed --   2.4  3   48  114736   11
Test (2B): agreement after follower reconnects ...
  ... Passed --   6.2  3  138   37601    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.6  5  232   48569    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.6  3   12    3480    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.4  3  194   49348    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  26.9  5 2372 1716724  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.3  3   46   13866   12
PASS
ok  	github.com/cyanial/raft	49.590s

real	0m49.914s
user	0m7.833s
sys     0m1.659s
```
 ### 2C

```bash
Test (2C): basic persistence ...
  ... Passed --   3.6  3   88   23139    6
Test (2C): more persistence ...
  ... Passed --  16.5  5 2632  382810   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.8  3   38   10273    4
Test (2C): Figure 8 ...
  ... Passed --  34.8  5 1468  316465   41
Test (2C): unreliable agreement ...
  ... Passed --   5.5  5  224   80429  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  42.3  5 3776 6206104  553
Test (2C): churn ...
  ... Passed --  16.3  5  684  436268  275
Test (2C): unreliable churn ...
  ... Passed --  16.3  5 1080  471265  207
PASS
ok  	github.com/cyanial/raft	140.676s

real	2m21.001s
user	0m56.675s
sys     0m7.351s
```

### 2D

```bash
Test (2D): snapshots basic ...
  ... Passed --   7.1  3  138   49414  194
Test (2D): install snapshots (disconnect) ...
  ... Passed --  53.9  3 1214  483194  350
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  61.4  3 1398  515575  285
Test (2D): install snapshots (crash) ...
  ... Passed --  39.7  3  712  367596  330
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  44.0  3 1034  414136  332
Test (2D): crash and restart all servers ...
  ... Passed --  13.4  3  260   78156   56
PASS
ok  	github.com/cyanial/raft	220.629s

real	3m41.212s
user	0m14.129s
sys     0m3.220s
```
