# FaultTolerantKVService
一种具有强一致性的分布式键值对存储系统，基于raft共识算法提供一种可靠的方式来存储需要由分布式系统或机器集群访问的数据。支持Get、Put、Append、Delete四种操作。
它可以在网络分区期间进行领导者选举，并可以容忍机器故障。Server支持grpc调用，具备密码认证、会话管理、重启恢复等特性；Client支持在备机间自动故障转移。

## Problems
在开发FT-KVServer时，存在很多问题，以下是我的思考

### Raft state 和 snapshot 如何持久化
如果在持久化 raft 的状态时，也必须得写入 snapshot 的话，当 snapshot 的体积变大时，这样的写入会越来越慢

写入策略：
单单持久化 raft state 时，可以在 .rf 文件写入，同时保存 raft state 的版本号，每次写入都会递增该版本号
同时持久化 raft state 和 snapshot 时，可以在 .rfs 文件写入，同时保存 raft state 的版本号

读取策略：
当需要读取 raft state 时，如果只存在 .rf 文件，则读取并返回
如果还存在.rfs 文件，则读取这两个文件中的 raft state，返回版本号更大的那一个
当需要读取 snapshot 时，则读取 .rfs 文件，并将其返回
.rfs文件内容格式如下：
RAFT -- file header（四个字节大小，string类型）
stateVersion -- version of raft state（8个字节大小，int64类型)
stateSize -- size of raft state（4个字节大小，int类型）
state(bytes) -- raft state（字节数组，stateSize字节）
snapshot(bytes) -- snapshot of service（字节数组，直到文件EOF）

先写在内存，然后定时将其写回磁盘可以提高性能，但是会牺牲一致性。
每次修改 raft state 时，都将其写回磁盘，可以提高这个分布式键值对存储系统的一致性。

### KVServer有过滤重复的相同请求的必要吗
我开发KVServer的想法是来自于 6.824#Lab3 的，正是因为做了这个实验，所以想在这个实验的基础上做一个mini版的键值对存储系统的。
代码是在Lab3的基础上进行开发的，Lab3是假设了一些前提的：一个客户端一次只会发送一个请求，请求没有完成时会一直重试。
基于这个假设，KVServer就需要过滤客户端相同且重复的请求。为此，可以使用ClientId+RequestId来标记客户端的请求。
客户端从KVServer拿到ClientId(单调递增，保证唯一)后，RequestId初始化为1，每次完成一个请求后，RequestId原子递增。
KVServer用于检测重复请求的代码如下：
```go
func (kv *KVServer) startApply() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			commandType := op.OpType
			requestId := op.RequestId
			// ...
				if id, exists := kv.requestMap[op.ClientId]; !exists || requestId > id {
					// ...
					kv.requestMap[op.ClientId] = requestId
				} else {
					log3A("[%d] duplicate %s request, requestId=%d", kv.me, op.OpType, op.RequestId)
				}
			}
			//...
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			//...
		} else {
			log.Fatalf("[%d] receive unknown type log!", kv.me)
		}
	}
}
```
但是很显然，实际使用KVServer时是不会这样的。实际上 如果请求迟迟没有完成，是不应该一直等待下去的，而是应该有一个超时时间。
创建的客户端可能会被多个使用者使用，同一时间可能会发出多个请求。 采用上面的ClientId+RequestId来检测重复请求是不行的。
如果要存储所有的ClientId+RequestId，不仅增加了系统复杂度，还增大了内存消耗。因此这段代码被移除。

### 如何验证客户端的身份


- **背景**：我们不希望自己的KVServer被随意访问，因此想要给KVServer加上一层认证措施，只有符合条件的客户端的请求才能被处理。
  这里当然会想到给KVServer设置密码，只有客户端提供正确的密码后，才是通过认证的客户端。
  在通过认证后，分配给客户端一个唯一的标识；客户端后续请求时就携带上这个标识，表明客户端已经通过认证了（因为不希望每次请求都要携带上密码）。
  参照Redis这样的设置，将密码保存在一个配置文件中，KVServer启动时就读取配置文件里保存的密码，并将其与客户端提供的密码进行核对。
- **问题**：标识应当无规律，难以通过暴力尝试手段得到正确的标识。怎样确保生成的客户端标识的是分布式唯一的？
- **思路**：B/S架构下的SessionId是一个参考，可以考虑给每个通过认证的Client生成一个唯一的SessionId(随机串，比如uuid)，根据客户端提供的SessionId参数来验证会话的有效性。
  但是在分布式情境下，即使是基于时间戳并在同一台机器上生成uuid，也是有重复的可能。一般采用的策略是是选择**雪花算法（SnowFlake）**，亦或者利用分布式锁来生成。
  基于Raft提供的强一致性保证，可以在集群中生成一个唯一的int64整数。我们可以将这个整数作为SessionId的前缀，uuid作为SessionId的后缀。
  而者通过非数字字符相连组成SessionId。 这样生成的uuid发生重复也是没关系的，因为前缀必定是不同的。

#### 细节问题：记录SessionId的数据需要持久化吗(写入到快照)?
可以不持久化，也就是说，Server对SessionId的记录是可以丢失的。

**首先考虑单Server的情况：**
当Server崩溃重启时，会读取log并重放，因此Client与Server通信会出现以下几种情况：
1. Client无法与Server通信，则Client将当前SessionId作废。
2. Client发送请求给Server，Server重放日志后，Server仍然有SessionId的记录，那么Server是可以处理Client的请求的(就好像Server没有崩溃一样)。
3. 否则此时SessionId是无效的，Server会拒绝Client的请求，因此Client也会将当前SessionId作废。

即使持久化了，还是会出现上面三种情况(假如Client给Server发请求时，Server还没有将日志完全重放，则SessionId还是无效的)
**Server崩溃了，Client与Server的连接也会断开，RPC调用就会直接失败，是否可以通过这个来直接作废SessionId？**
对于作废的SessionId，由go routine定时清理

**再考虑集群的情况：**
集群相较于单主机可能会复杂一点，但是有一点可以明确：只有Leader才能处理Client的请求。Leader是集群中log最为完整的。
基于Raft提供的强一致性保证，如果Leader没有发生切换，则Client发送给Leader的请求，情况和单Server是一样的。
假如当前Leader崩溃了，那么Client会找到新的Leader，而该Leader的日志至少与前Leader一样新，因此情况和单Server还是一样的。

再思考深一点，如果就是想保证生成的uuid就是唯一的呢，可否用现有的Raft做到？



## 客户端看到的故障模型
KVServer依靠Raft共识算法来达到强一致性，对抗网络分区、宕机等情况。KVServer对外暴露接口供客户端进行RPC调用。
一般情况下，KVServer是以集群的形式存在的，而根据Raft共识算法，只有集群中的Leader才能处理请求。
因此对KVServer的RPC调用在一开始很可能不会成功，所以需要对客户端进行一定的封装，才能更方便地使用KVServer。
在封装Client的过程中，需要给Client的使用者提供一个一致的错误模型。
对KVServer的RPC调用可能出现以下情况：
1. KVServer宕机或Client无法连接到KVServer时，RPC调用无响应。
2. KVServer不是Leader
3. KVServer是Leader并提交了客户端的命令，但可能由于网络延迟等原因，导致命令在很长一段时间内都没有执行完成(也就是没有达成共识)。
4. 客户端的请求执行成功

当有多个KVServer时，对于第一、二种情况来说，Client应尝试调用其他KVServer，只有调用过其他KVServer也无法找到Leader时，才应当认定服务器出现故障。
而对于第三种情况来说，客户端的请求可能会执行也可能不会执行；对客户端而言，命令没有达成共识和网络延迟是一样的情况，请求是否执行对于客户端来说也是不确定的。

## 有哪些地方是可以改进的？
1. 关于提供持久化RaftState和Snapshot的Storage，写入磁盘是调用的OS的sync，而像MySQL这种数据库是自己实现了磁盘的写入，更快。

## 比较特别的地方

### 1. 设计请求处理流程

在处理请求方面，基于Raft的KVServer相较于传统的KVServer有很大不同。KVServer是需要等待命令达成共识才能执行请求的。
KVServer将Client的请求包装为一个Command提交给Raft， Raft会将达成共识的KVServer通过Channel发送给KVServer。
这里就引出了一个问题，对于每个请求，KVServer如何知晓这个请求执行是否成功呢？
在Service向Raft提交Command时，RaftCommand包装为log，并会返回对应log的`index`和`term`；根据Raft共识算法，index和term确定了log的唯一性（阅读论文后可知）。
因此KVServer可以通过`index`来等待请求执行完成的`signal`，假如回传的命令的term与等待的不符，则说明等待的命令没有达成共识。
在这里我使用的还是go中的`channel(chan)`，KVServer使用map数据结构记录等待中的`channel`（map使用方便）。

KVServer处理Raft回传的Command程序如下，从applyCh接收命令，根据命令类型执行相应的操作。
同时会判断对应`index`是否有`channel`正在等待；有的话就回传ApplyResult(包含了Term)，随后从map中删除相关记录，最后close。
尽量减少不必要的存储。
```go
// startApply listen to the log sent from applyCh and execute the corresponding command.
func (kv *KVServer) startApply() {
	for {
		msg := <- kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			commandType := op.OpType
			requestId := op.RequestId
			result := ApplyResult{
				Term: msg.CommandTerm,
			}
			// ...
			// ..。
			if pb.Op_PUT == commandType {
				kv.tab[op.Key] = op.Value
			} else if pb.Op_APPEND == commandType {
				v := kv.tab[op.Key]
				v += op.Value
				kv.tab[op.Key] = v
			} else if pb.Op_DELETE == commandType {
				delete(kv.tab, op.Key)
			} else if GET != commandType {
			}
			kv.commitIndex = msg.CommandIndex
			if ch, _ := kv.replyChan[kv.commitIndex]; ch != nil {
				ch <- result
				close(ch)
				delete(kv.replyChan, kv.commitIndex)
			}
			// ...
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// snapshot...
		} else {
			log.Fatalf("[%d] receive unknown type log!", kv.me)
		}
	}
}
```

对于命令的处理流程，前后修改过很多，两个版本都是直接用本地变量`ch`来接收`signal`，而不是再用map中的`channel`(方便清理map中不用的channel)
只有Leader能提交请求，提交请求后会设置相应的`channel`，并让线程等待直到超时。
#### 第一版
```go
func (kv *KVServer) submit(op Op) (*ApplyResult, pb.ErrCode) {
	commandIndex, commandTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, pb.ErrCode_WRONG_LEADER
	}

	kv.mu.Lock()
	if c, _ := kv.replyChan[commandIndex]; c != nil {
		kv.mu.Unlock()
		return nil, pb.ErrCode_TIMEOUT
	}
	ch := make(chan ApplyResult, 1)
	kv.replyChan[commandIndex] = ch
	kv.mu.Unlock()

	var res ApplyResult
	select {
	case res = <-ch:
		break
	case <-time.After(RequestTimeout):
		kv.mu.Lock()
		if _, deleted := kv.replyChan[commandIndex]; deleted {
			kv.mu.Unlock()
			res = <-ch
			break
		}
		delete(kv.replyChan, commandIndex)
		kv.mu.Unlock()
		close(ch)
		return nil, errCode
	}
	if res.Term == commandTerm {
		return &res, pb.ErrCode_OK
	} else {
		return nil, pb.ErrCode_WRONG_LEADER
	}
}
```
第一版首先有个问题，超时时间不应该由KVServer来决定，而应该由Client来决定。
另外，还有一个严重的问题，那就是在 `c, _ := kv.replyChan[commandIndex]; c != nil` 这个if语句没有考虑清楚。
思考一下，当多个Client向Leader提交请求时获得的`commandIndex`会不会相同。
设leader1是`term1`的leader，假如出现了网络分区（Server之间的网络存在故障）且Leader1不处于主分区（它和绝大多数Server通信存在网络故障）。
Leader1仍然认为自己是leader（而此时主分区在`term2`选举出了leader2，term2 > term1)，并提交来自客户端的请求，很明显，这些请求不会commit。
leader2在commit一些命令后，与leader1的通信恢复正常。按照Raft共识算法，leader1会trim掉与leader2发生冲突的log，并append来自leader2的log。
只要append的没有trim掉的多，也就说明leader1的log长度减小了。leader1在`term3`重新成为leader，则会上述情况。
这种情况一出现，就说明先前客户端的命令不可能commit；这时，只需要回传一个result（回传的term必定大于前面等待term)即可。
#### 第二版
```go
func (kv *KVServer) submit(op Op) (*ApplyResult, pb.ErrCode) {
	commandIndex, commandTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, pb.ErrCode_WRONG_LEADER
	}
	kv.mu.Lock()
	if c, _ := kv.replyChan[commandIndex]; c != nil {
		c <- ApplyResult{Term: commandTerm}
		close(c)
	}
	ch := make(chan ApplyResult, 1)
	kv.replyChan[commandIndex] = ch
	kv.mu.Unlock()

	res := <-ch
	if res.Term == commandTerm {
		return &res, pb.ErrCode_OK
	} else {
		return nil, pb.ErrCode_WRONG_LEADER
	}
}
```
