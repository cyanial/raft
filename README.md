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
 