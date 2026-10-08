package jobqueue

// taskGroup 保存同一个业务键下等待有序消费的任务。
type taskGroup struct {
	// queue 是已入队但尚未进入 ready 通道的任务，按提交先后排列。
	// 仅当 active 为 false 时，queue[0] 才可被下发；其余任务严格被头任务阻塞。
	queue []*Task
	// active 表示该组已有一个任务离开 queue（在 ready 通道中，或处于
	// RUNNING / RETRY_WAIT）。同组在它进入终态前不会下发第二个。
	active bool
}

// groupRegistry 管理业务键 -> 分组。所有访问由 Queue.mu 串行化。
type groupRegistry struct {
	groups map[string]*taskGroup
}

func newGroupRegistry() *groupRegistry {
	return &groupRegistry{groups: make(map[string]*taskGroup)}
}

// admitLocked 把一个带业务键的新任务（或重投任务）放入对应分组。
// 若该组当前没有在途头任务，则把它标记为 active 并返回它供调用方下发到 ready；
// 否则任务留在组队列中等待头任务结束，返回 nil。
// 调用方必须持有 Queue.mu。
func (q *Queue) admitLocked(t *Task) *Task {
	if t.Key == "" {
		// 无业务键：不受分组约束，直接下发。
		return t
	}
	g := q.groups.groups[t.Key]
	if g == nil {
		g = &taskGroup{}
		q.groups.groups[t.Key] = g
	}
	if g.active {
		g.queue = append(g.queue, t)
		return nil
	}
	g.active = true
	return t
}

// releaseGroupLocked 在一个带键任务进入终态后调用：弹出组内下一个任务并标记
// active 后返回供调用方下发；组内已无任务时删除分组并返回 nil。
//
// 停机排空阶段同样推进：worker 正在非阻塞排空 ready，让同组后续任务也被
// 优雅执行；若此刻已无消费者领取（deliver 被 shutdown 拦截），残留任务由
// finalize 最终兜底统一转为 PENDING_RETRY。
// 调用方必须持有 Queue.mu。
func (q *Queue) releaseGroupLocked(key string) *Task {
	if key == "" {
		return nil
	}
	g := q.groups.groups[key]
	if g == nil {
		return nil
	}
	if len(g.queue) == 0 {
		// 没有后续任务：整个分组生命周期结束，删除以免 map 无限增长。
		delete(q.groups.groups, key)
		return nil
	}
	next := g.queue[0]
	g.queue = g.queue[1:]
	g.active = true
	return next
}
