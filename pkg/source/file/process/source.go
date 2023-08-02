package process

import (
	"errors"
	"io"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/file"
)

func init() {
	file.RegisterProcessor(makeSource)
	prometheus.MustRegister(fileReadLatency)
	prometheus.MustRegister(sourceProcessorLatency)
}

var fileReadLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "loggie_debug_file_read_latency",
		Help:    "file read latency in seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 10),
	}, []string{"filename"},
)

var sourceProcessorLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "loggie_debug_reader_source_processor_latency",
		Help:    "source processor latency in seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 10),
	}, []string{"filename"},
)

func makeSource(config file.ReaderConfig) file.Processor {
	return &SourceProcessor{
		readBufferSize: config.ReadBufferSize,
	}
}

type SourceProcessor struct {
	readBufferSize int
}

func (sp *SourceProcessor) Order() int {
	return 300
}

func (sp *SourceProcessor) Code() string {
	return "source"
}

func (sp *SourceProcessor) Process(processorChain file.ProcessChain, ctx *file.JobCollectContext) {
	job := ctx.Job
	ctx.ReadBuffer = ctx.ReadBuffer[:sp.readBufferSize]
	startAt := time.Now()
	l, err := job.File().Read(ctx.ReadBuffer)
	fileReadLatency.With(prometheus.Labels{
		"filename": ctx.Filename,
	}).Observe(time.Since(startAt).Seconds())
	if errors.Is(err, io.EOF) || l == 0 {
		ctx.IsEOF = true
		job.EofCount++
		return
	}
	if err != nil {
		ctx.IsEOF = true
		log.Error("file(name:%s) read fail: %v", ctx.Filename, err)
		return
	}
	read := int64(l)
	ctx.ReadBuffer = ctx.ReadBuffer[:read]

	if !strings.HasSuffix(ctx.Filename, "loggie.log") {
		log.Info("!!DEBUG: filename=%s: read %d bytes ", ctx.Filename, read)
	}
	sourceProcessorLatency.With(prometheus.Labels{
		"filename": ctx.Filename,
	}).Observe(time.Since(startAt).Seconds())
	// see lineProcessor.Process
	processorChain.Process(ctx)
}
