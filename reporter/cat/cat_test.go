package cat

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/snowflake"
	"github.com/pkg/errors"
)

func TestUint(t *testing.T) {
	err := errors.New("dsad")
	s := strings.Trim(fmt.Sprintf("%+v", err), "\n")
	fmt.Println(s)
}

func TestCatReporter(t *testing.T) {
	reporter := NewReporter("cat-e", WorkersOption(1))
	defer reporter.Close()

	globalTicket, _ := snowflake.GenerateID()
	traceId, _ := snowflake.GenerateID()

	for i := 0; i < 100; i++ {
		span := model.SpanModel{
			SpanContext: model.SpanContext{
				Err: errors.New("I m error"),
			},
			TraceContext: model.TraceContext{
				GlobalTicket: globalTicket.String(),
				TraceId:      traceId.String(),
			},
			Name:      "EXCEPTION",
			Value:     "exception",
			Timestamp: time.Now(),
			Duration:  60 * time.Microsecond,
		}

		reporter.Send(span)
		time.Sleep(time.Second)
	}

	time.Sleep(5 * time.Minute)
}
