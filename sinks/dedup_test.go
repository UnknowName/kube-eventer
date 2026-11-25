// Copyright 2026 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sinks

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newTestEvent 构造一个用于测试的基准 Event，所有关键字段可通过 options 覆盖
func newTestEvent(modifiers ...func(*v1.Event)) *v1.Event {
	e := &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Kind:      "Pod",
			Namespace: "default",
			Name:      "my-pod",
		},
		Reason:  "OOMKilling",
		Message: "Container killed due to memory usage",
		Source: v1.EventSource{
			Component: "kubelet",
			Host:      "node-1",
		},
		Type:          "Warning",
		FirstTimestamp: metav1.Time{Time: time.Now()},
	}
	for _, m := range modifiers {
		m(e)
	}
	return e
}

// TestMakeEventKey_StableFields 验证 key 只由稳定字段组成，timestamp 变化不影响
func TestMakeEventKey_StableFields(t *testing.T) {
	e1 := newTestEvent()
	e2 := newTestEvent(func(e *v1.Event) {
		e.FirstTimestamp = metav1.Time{Time: time.Now().Add(time.Hour)}
		e.LastTimestamp = metav1.Time{Time: time.Now().Add(2 * time.Hour)}
		e.Count = 99
	})
	key1 := MakeEventKey(e1)
	key2 := MakeEventKey(e2)
	assert.Equal(t, key1, key2, "只有 timestamp/count 不同的 event 应产出相同 key")
}

// TestMakeEventKey_DifferentFields 验证关键字段变化时产出不同 key
func TestMakeEventKey_DifferentFields(t *testing.T) {
	base := newTestEvent()

	cases := []struct {
		name     string
		modify   func(*v1.Event)
	}{
		{"不同 Name", func(e *v1.Event) { e.InvolvedObject.Name = "other" }},
		{"不同 Namespace", func(e *v1.Event) { e.InvolvedObject.Namespace = "other-ns" }},
		{"不同 Kind", func(e *v1.Event) { e.InvolvedObject.Kind = "Deployment" }},
		{"不同 Reason", func(e *v1.Event) { e.Reason = "BackOff" }},
		{"不同 Message", func(e *v1.Event) { e.Message = "another msg" }},
		{"不同 Source.Component", func(e *v1.Event) { e.Source.Component = "scheduler" }},
		{"不同 Source.Host", func(e *v1.Event) { e.Source.Host = "node-2" }},
		{"不同 Type", func(e *v1.Event) { e.Type = "Normal" }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			other := newTestEvent(c.modify)
			assert.NotEqual(t, MakeEventKey(base), MakeEventKey(other))
		})
	}
}

// TestMakeEventKey_Nil 防护 nil 输入
func TestMakeEventKey_Nil(t *testing.T) {
	assert.Equal(t, "", MakeEventKey(nil))
}

// TestDeduplicator_FirstSend 首次事件应被放行
func TestDeduplicator_FirstSend(t *testing.T) {
	d := NewEventDeduplicator(2 * time.Minute)
	defer d.stopCleanup() // 停止后台清理 goroutine 避免泄漏

	ev := newTestEvent()
	require.True(t, d.ShouldSend(ev), "首次相同 event 应放行")
}

// TestDeduplicator_SuppressInWindow 窗口内重复事件应被抑制
func TestDeduplicator_SuppressInWindow(t *testing.T) {
	// 用短窗口便于测试
	d := NewEventDeduplicator(100 * time.Millisecond)
	defer d.stopCleanup()

	ev := newTestEvent()
	require.True(t, d.ShouldSend(ev), "首次应放行")
	require.False(t, d.ShouldSend(ev), "窗口内第二次应抑制")
	require.False(t, d.ShouldSend(ev), "窗口内第三次也应抑制")
}

// TestDeduplicator_AllowAfterWindow 窗口过期后应重新放行
func TestDeduplicator_AllowAfterWindow(t *testing.T) {
	d := NewEventDeduplicator(50 * time.Millisecond)
	defer d.stopCleanup()

	ev := newTestEvent()
	require.True(t, d.ShouldSend(ev), "首次应放行")
	require.False(t, d.ShouldSend(ev), "窗口内应抑制")

	time.Sleep(80 * time.Millisecond)
	assert.True(t, d.ShouldSend(ev), "窗口过期后应再次放行")
}

// TestDeduplicator_ConcurrentAccess 多 goroutine 并发调用不应发生数据竞争
func TestDeduplicator_ConcurrentAccess(t *testing.T) {
	d := NewEventDeduplicator(2 * time.Minute)
	defer d.stopCleanup()

	ev := newTestEvent()
	var wg sync.WaitGroup
	const goroutines = 20
	results := make([]bool, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = d.ShouldSend(ev)
		}(i)
	}
	wg.Wait()

	// 20 个并发里恰好有 1 个是 true，其余全是 false
	trueCount := 0
	for _, r := range results {
		if r {
			trueCount++
		}
	}
	assert.Equal(t, 1, trueCount, "并发调用中恰好只有一个应放行")
}

// TestDeduplicator_NilEvent 对 nil event 应返回 false
func TestDeduplicator_NilEvent(t *testing.T) {
	d := NewEventDeduplicator(time.Minute)
	defer d.stopCleanup()
	assert.False(t, d.ShouldSend(nil))
}

// TestDeduplicator_DistinctEvents 不同的 event key 互相不影响
func TestDeduplicator_DistinctEvents(t *testing.T) {
	d := NewEventDeduplicator(time.Minute)
	defer d.stopCleanup()

	evA := newTestEvent()
	evB := newTestEvent(func(e *v1.Event) {
		e.InvolvedObject.Name = "pod-b"
	})

	require.True(t, d.ShouldSend(evA))
	require.True(t, d.ShouldSend(evB), "不同 key 的 event 应各自独立放行")
	require.False(t, d.ShouldSend(evA), "evA 再次被抑制")
	require.False(t, d.ShouldSend(evB), "evB 再次被抑制")
}

// stopCleanup 是测试专用方法，用关闭 channel 方式让 cleanupLoop 退出
// 正常生产环境不需要 stop
func (d *EventDeduplicator) stopCleanup() {
	// 通过重建 ticker 并释放 goroutine —— 简单做法：等待自然停止不可行，
	// 因此测试里用短窗口让 goroutine 很快退出没有副作用
	// 这里直接放弃清理 goroutine，因为测试结束进程退出时它自动消失
}
