# Raft Impl

## Leader election

节点的身份:

- `Follower`
- `Candidate`
- `Leader`

心跳包(`heartbeats`): `AppendEntries RPC`(不包含`log entries`)

### 节点运行状态

1. 节点初始化为`follower`
2. 接受来自 `leader` 的心跳包 (loop-timeout)
3. 接受心跳包超时(`election timeout`)
4. 开始选举, 变成`Candidate`, `currentTerm+1`, 投票给自己, 发送`RequestVote RPC`
5. (a)成为`leader`,(b)成为`follower`,(c)选举超时

(a) 得到大多数投票
(b) 其他任何节点成为`leader`
 
 ## 测试结果

 ### 2A

 ```sh
Test (2A): initial election ...
  ... Passed --   3.1  3   60   17666    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  138   28041    0
Test (2A): multiple elections ...
  ... Passed --   5.5  7  582  124881    0
PASS
ok  	github.com/cyanial/raft	13.493s
go test -race -run 2A  1.22s user 0.58s system 13% cpu 13.829 total
 ```

 ### 2B

```bash
Test (2B): basic agreement ...
  ... Passed --   0.8  3   16    4612    3
Test (2B): RPC byte count ...
  ... Passed --   2.5  3   50  115234   11
Test (2B): agreement after follower reconnects ...
  ... Passed --   6.0  3  132   36223    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.5  5  224   45944    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.6  3   12    3480    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   4.1  3  148   35919    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  26.9  5 2364 1724615  103
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.1  3   42   12670   12
PASS
ok  	github.com/cyanial/raft	46.832s
go test -race -run 2B  4.87s user 1.13s system 12% cpu 47.156 total
```
 ### 2C

```bash
Test (2C): basic persistence ...
  ... Passed --   4.1  3   92   24383    6
Test (2C): more persistence ...
  ... Passed --  20.5  5 3720  522258   18
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.8  3   38   10036    4
Test (2C): Figure 8 ...
  ... Passed --  37.0  5 1524  328944   41
Test (2C): unreliable agreement ...
  ... Passed --   7.4  5  300  101820  251
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  41.0  5 3948 7019263  360
Test (2C): churn ...
  ... Passed --  16.6  5  844  418124  292
Test (2C): unreliable churn ...
  ... Passed --  16.3  5  712  207715  165
PASS
ok  	github.com/cyanial/raft	145.569s
go test -race -run 2C  43.72s user 5.52s system 33% cpu 2:25.91 total
```
 ### 2D