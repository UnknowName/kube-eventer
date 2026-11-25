// Copyright 2015 Google Inc. All Rights Reserved.
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
	"fmt"
	"log"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/AliyunContainerService/kube-eventer/core"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

const (
	DefaultSinkExportEventsTimeout = 20 * time.Second
	DefaultSinkStopTimeout         = 60 * time.Second
)

var (
	// Time spent exporting events to sink in milliseconds.
	exporterDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "eventer",
			Subsystem: "exporter",
			Name:      "duration_milliseconds",
			Help:      "Time spent exporting events to sink in milliseconds.",
		},
		[]string{"exporter"},
	)
)

func init() {
	prometheus.MustRegister(exporterDuration)
}

type sinkHolder struct {
	sink              core.EventSink
	eventBatchChannel chan *core.EventBatch
	stopChannel       chan bool
}

// Sink Manager - a special sink that distributes data to other sinks. It pushes data
// only to these sinks that completed their previous exports. Data that could not be
// pushed in the defined time is dropped and not retried.
type sinkManager struct {
	sinkHolders         []sinkHolder
	exportEventsTimeout time.Duration
	// Should be larger than exportEventsTimeout, although it is not a hard requirement.
	stopTimeout time.Duration
}

func NewEventSinkManager(sinks []core.EventSink, exportEventsTimeout, stopTimeout time.Duration) (core.EventSink, error) {
	sinkHolders := []sinkHolder{}
	for _, sink := range sinks {
		sh := sinkHolder{
			sink:              sink,
			eventBatchChannel: make(chan *core.EventBatch),
			stopChannel:       make(chan bool),
		}
		sinkHolders = append(sinkHolders, sh)
		go func(sh sinkHolder) {
			for {
				select {
				case data := <-sh.eventBatchChannel:
					export(sh.sink, data)
				case isStop := <-sh.stopChannel:
					klog.V(2).Infof("Stop received: %s", sh.sink.Name())
					if isStop {
						sh.sink.Stop()
						return
					}
				}
			}
		}(sh)
	}
	return &sinkManager{
		sinkHolders:         sinkHolders,
		exportEventsTimeout: exportEventsTimeout,
		stopTimeout:         stopTimeout,
	}, nil
}

// Guarantees that the export will complete in exportEventsTimeout.
func (this *sinkManager) ExportEvents(data *core.EventBatch) {
	var wg sync.WaitGroup
	for _, sh := range this.sinkHolders {
		wg.Add(1)
		go func(sh sinkHolder, wg *sync.WaitGroup) {
			defer wg.Done()
			klog.V(2).Infof("Pushing events to: %s", sh.sink.Name())
			select {
			case sh.eventBatchChannel <- data:
				klog.V(2).Infof("Data events completed: %s", sh.sink.Name())
				// everything ok
			case <-time.After(this.exportEventsTimeout):
				klog.Warningf("Failed to events data to sink: %s", sh.sink.Name())
			}
		}(sh, &wg)
	}
	// Wait for all pushes to complete or timeout.
	wg.Wait()
}

func (this *sinkManager) Name() string {
	return "Manager"
}

func (this *sinkManager) Stop() {
	for _, sh := range this.sinkHolders {
		klog.V(2).Infof("Running stop for: %s", sh.sink.Name())

		go func(sh sinkHolder) {
			select {
			case sh.stopChannel <- true:
				// everything ok
				klog.V(2).Infof("Stop sent to sink: %s", sh.sink.Name())

			case <-time.After(this.stopTimeout):
				klog.Warningf("Failed to stop sink: %s", sh.sink.Name())
			}
			return
		}(sh)
	}
}

// allowedReasons 定义需要关注的异常事件 Reason 白名单
// kubelet 官方常量见 pkg/kubelet/events/event.go
// OOMKilling / FailedScheduling / FailedCreate 为各组件直接 emit 的字符串
var allowedReasons = map[string]struct{}{
	"BackOff":          {}, // kubelet: 容器/镜像反复启动失败，进入退避
	"Killing":          {}, // kubelet: 容器被终止
	"OOMKilling":       {}, // kubelet: cgroup OOM Killer 触发（资源超限核心信号）
	"Unhealthy":        {}, // kubelet: 存活/就绪探针失败
	"FailedCreate":     {}, // controller-manager: 创建 Pod 失败（含配额超限）
	"FailedScheduling": {}, // scheduler: Pod 调度失败（含资源不足）
}

// EventDeduplicator 实现基于时间窗口的 Event 发送抑制
// 在冷却窗口内，相同 Event（除 timestamp 外字段一致）只被发送一次
type EventDeduplicator struct {
	mu sync.Mutex
	// 上次被"放行"的 event key → 发送时间
	sentAt map[string]time.Time
	window time.Duration // 冷却窗口时长
}

// NewEventDeduplicator 创建一个指定窗口时长的去重器
func NewEventDeduplicator(window time.Duration) *EventDeduplicator {
	d := &EventDeduplicator{
		sentAt: make(map[string]time.Time),
		window: window,
	}
	// 启动后台清理，防止 map 无限增长
	go d.cleanupLoop()
	return d
}

// MakeEventKey 根据 Event 的稳定字段生成唯一标识，刻意排除 FirstTimestamp/LastTimestamp/EventTime
// 相同"事件类型"的 Event 应产出相同 key
func MakeEventKey(e *v1.Event) string {
	if e == nil {
		return ""
	}
	io := e.InvolvedObject
	return fmt.Sprintf("%s/%s/%s|%s|%s|%s|%s|%s",
		io.Kind,
		io.Namespace,
		io.Name,
		e.Reason,
		e.Message,
		e.Source.Component,
		e.Source.Host,
		e.Type,
	)
}

// ShouldSend 判断该 Event 是否在冷却窗口内首次出现
// 返回 true 表示应该放行；同时会更新发送时间
func (d *EventDeduplicator) ShouldSend(e *v1.Event) bool {
	if e == nil {
		return false
	}
	key := MakeEventKey(e)

	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if lastSent, ok := d.sentAt[key]; ok {
		if now.Sub(lastSent) < d.window {
			// 窗口内重复事件，抑制发送
			return false
		}
	}
	// 放行并记录时间戳
	d.sentAt[key] = now
	return true
}

// cleanupLoop 定期清理已过期的 entry，防止内存泄漏
func (d *EventDeduplicator) cleanupLoop() {
	ticker := time.NewTicker(d.window)
	defer ticker.Stop()
	for range ticker.C {
		d.mu.Lock()
		cutoff := time.Now().Add(-d.window)
		for k, v := range d.sentAt {
			if v.Before(cutoff) {
				delete(d.sentAt, k)
			}
		}
		d.mu.Unlock()
	}
}

// eventDeduplicator 是进程级单例，在 sinks manager 初始化时创建
var eventDeduplicator = NewEventDeduplicator(2 * time.Minute)

func export(s core.EventSink, data *core.EventBatch) {
	startTime := time.Now()
	defer func() {
		exporterDuration.
			WithLabelValues(s.Name()).
			Observe(float64(time.Since(startTime)) / float64(time.Millisecond))
	}()

	filtered := core.EventBatch{Timestamp: data.Timestamp, Events: make([]*v1.Event, 0)}
	for _, ev := range data.Events {
		// 第一步：按 Reason 白名单过滤，只关注异常事件
		klog.Info("**message:", ev.Message, "**reason:", ev.Reason, "**count:", ev.Count)
		if _, ok := allowedReasons[ev.Reason]; !ok {
			continue
		}
		// 第二步：2分钟窗口内相同 Event 只发一次（抑制重复告警）
		if !eventDeduplicator.ShouldSend(ev) {
			klog.V(2).Infof("抑制重复 event: reason=%s obj=%s/%s",
				ev.Reason, ev.InvolvedObject.Kind, ev.InvolvedObject.Name)
			continue
		}

		filtered.Events = append(filtered.Events, ev)
	}

	log.Printf("通过Sink %s 发送消息%d条", s.Name(), len(filtered.Events))
	s.ExportEvents(&filtered)
}
