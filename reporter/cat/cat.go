package cat

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lyouthzzz/cat-go/cat"
	"github.com/lyouthzzz/cat-go/message"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter"
)

var _ reporter.Reporter = &catReporter{}

type Option func(*catReporter)

func WorkersOption(workders int) Option {
	return func(reporter *catReporter) {
		reporter.workers = workders
	}
}

type catReporter struct {
	domain  string
	workers int
	spanC   chan *model.SpanModel
	stopC   chan struct{}
	wg      sync.WaitGroup
}

func NewReporter(domain string, opts ...Option) reporter.Reporter {

	reporter := &catReporter{
		domain:  domain,
		workers: 1,
	}

	for _, opt := range opts {
		opt(reporter)
	}

	reporter.spanC = make(chan *model.SpanModel, 1024)
	reporter.stopC = make(chan struct{})

	// init cat client
	cat.Init(reporter.domain)

	reporter.workAsync()

	return reporter
}

func (r *catReporter) Send(span model.SpanModel) {
	r.spanC <- &span
}

func (r *catReporter) Close() error {
	// close channel 再发送数据会导致panic 。必须先终止发送者的写请求，再close channel
	close(r.spanC)

	timeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		r.wg.Wait()
		r.stopC <- struct{}{}
	}()

	select {
	case <-r.stopC:
		return nil
	case <-timeout.Done():
		return errors.New("cat reporter close timeout")
	}
}

func (r *catReporter) workAsync() {
	for i := 0; i < r.workers; i++ {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.send()
		}()
	}
}

func (r *catReporter) send() {
	for span := range r.spanC {
		t := cat.NewTransaction(span.Name, span.Value)

		t.SetStatus(message.CatSuccess)
		t.SetDurationStart(span.Timestamp)
		t.SetDuration(span.Duration)

		for k, v := range span.Tags {
			t.AddData(k, v)
		}
		if span.GlobalTicket != "" {
			t.AddData(GlobalTicket, span.GlobalTicket)
		}
		if span.TraceId != "" {
			t.AddData(TraceId, span.TraceId)
		}
		if span.RPCId != "" {
			t.AddData(RPCId, span.RPCId)
		}
		if span.RPCIndex != "" {
			t.AddData(RPCIndex, span.RPCIndex)
		}
		if span.RPCParentId != "" {
			t.AddData(RPCParentId, span.RPCParentId)
		}
		if span.RPCEntryURL != "" {
			t.AddData(RPCEntryURL, span.RPCEntryURL)
		}
		if span.MonitorKey != "" {
			t.AddData(MonitorKey, span.MonitorKey)
		}

		if span.Err != nil {
			t.SetStatus(message.CatFail)
			t.LogEvent("error", strings.Trim(fmt.Sprintf("%+v", span.Err), "\n"))
		}

		t.Complete()
	}
}
