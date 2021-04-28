module github.com/openzipkin/zipkin-go

go 1.16

require (
	github.com/Shopify/sarama v1.19.0
	github.com/Shopify/toxiproxy v2.1.4+incompatible // indirect
	github.com/eapache/go-resiliency v1.1.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/golang/protobuf v1.4.2
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/lyouthzzz/cat-go v2.0.0+incompatible
	github.com/onsi/ginkgo v1.7.0
	github.com/onsi/gomega v1.4.3
	github.com/pierrec/lz4 v1.0.2-0.20190131084431-473cd7ce01a1 // indirect
	github.com/pkg/errors v0.9.1
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a // indirect
	github.com/streadway/amqp v0.0.0-20190404075320-75d898a42a94
	github.com/stretchr/testify v1.7.0
	google.golang.org/grpc v1.30.0
	google.golang.org/protobuf v1.25.0
)

replace github.com/lyouthzzz/cat-go => ../../lyouthzzz/cat-go
