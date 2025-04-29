// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package influxdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/influx2otel"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"

	"github.com/domleb/opentelemetry-collector-contrib/internal/common/sanitize"
)

type metricsReceiver struct {
	nextConsumer       consumer.Metrics
	httpServerSettings *confighttp.ServerConfig
	converter          *influx2otel.LineProtocolToOtelMetrics

	server *http.Server
	wg     sync.WaitGroup

	logger common.Logger

	obsrecv *receiverhelper.ObsReport

	settings component.TelemetrySettings

	config *Config
}

func newMetricsReceiver(config *Config, settings receiver.Settings, nextConsumer consumer.Metrics) (*metricsReceiver, error) {
	influxLogger := newZapInfluxLogger(settings.TelemetrySettings.Logger)
	converter, err := influx2otel.NewLineProtocolToOtelMetrics(influxLogger)
	if err != nil {
		return nil, err
	}
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "http",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, err
	}

	return &metricsReceiver{
		nextConsumer:       nextConsumer,
		httpServerSettings: &config.ServerConfig,
		converter:          converter,
		logger:             influxLogger,
		obsrecv:            obsrecv,
		settings:           settings.TelemetrySettings,
		config:             config,
	}, err
}

func (r *metricsReceiver) Start(ctx context.Context, host component.Host) error {
	ln, err := r.httpServerSettings.ToListener(ctx)
	if err != nil {
		return fmt.Errorf("failed to bind to address %s: %w", r.httpServerSettings.Endpoint, err)
	}

	router := http.NewServeMux()
	router.HandleFunc("/write", r.handleWrite)        // InfluxDB 1.x
	router.HandleFunc("/api/v2/write", r.handleWrite) // InfluxDB 2.x
	router.HandleFunc("/ping", r.handlePing)

	r.wg.Add(1)
	r.server, err = r.httpServerSettings.ToServer(ctx, host, r.settings, router)
	if err != nil {
		return err
	}
	go func() {
		defer r.wg.Done()
		if errHTTP := r.server.Serve(ln); !errors.Is(errHTTP, http.ErrServerClosed) && errHTTP != nil {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errHTTP))
		}
	}()

	return nil
}

func (r *metricsReceiver) Shutdown(_ context.Context) error {
	if r.server == nil {
		return nil
	}
	if err := r.server.Close(); err != nil {
		return err
	}
	r.wg.Wait()
	return nil
}

const (
	defaultPrecision = lineprotocol.Nanosecond
	dataFormat       = "influxdb"
)

var precisions = map[string]lineprotocol.Precision{
	"ns": lineprotocol.Nanosecond,
	"n":  lineprotocol.Nanosecond,
	"µs": lineprotocol.Microsecond,
	"µ":  lineprotocol.Microsecond,
	"us": lineprotocol.Microsecond,
	"u":  lineprotocol.Microsecond,
	"ms": lineprotocol.Millisecond,
	"s":  lineprotocol.Second,
}

func (r *metricsReceiver) handleWrite(w http.ResponseWriter, req *http.Request) {
	defer func() {
		_ = req.Body.Close()
	}()

	precision := defaultPrecision
	if precisionStr := req.URL.Query().Get("precision"); precisionStr != "" {
		var ok bool
		if precision, ok = precisions[precisionStr]; !ok {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "unrecognized precision '%s'", sanitize.String(precisionStr))
			return
		}
	}

	batch := r.converter.NewBatch()
	lpDecoder := lineprotocol.NewDecoder(req.Body)

	ctx := r.obsrecv.StartMetricsOp(req.Context())

	var k, vTag []byte
	var vField lineprotocol.Value

	line := 0
	batchDrops := NewBatchDrops(r.config.MaxTrackedErrors)

	for ; lpDecoder.Next(); line++ {
		measurement, err := lpDecoder.Measurement()
		if err != nil {
			if !r.config.EnablePartialWrites {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = fmt.Fprintf(w, "failed to parse measurement on line %d", line)
				return
			}
			batchDrops.AddDrop(FailedMeasurement, line)
			continue
		}

		tags := make(map[string]string)
		for k, vTag, err = lpDecoder.NextTag(); k != nil && err == nil; k, vTag, err = lpDecoder.NextTag() {
			tags[string(k)] = string(vTag)
		}
		if err != nil {
			if !r.config.EnablePartialWrites {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = fmt.Fprintf(w, "failed to parse tag for '%s' on line %d", measurement, line)
				return
			}
			batchDrops.AddDrop(FailedTag, line, string(measurement))
			continue
		}

		fields := make(map[string]any)
		for k, vField, err = lpDecoder.NextField(); k != nil && err == nil; k, vField, err = lpDecoder.NextField() {
			fields[string(k)] = vField.Interface()
		}
		if err != nil {
			if !r.config.EnablePartialWrites {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = fmt.Fprintf(w, "failed to parse field for '%s' on line %d", measurement, line)
				return
			}
			batchDrops.AddDrop(FailedField, line, string(measurement))
			continue
		}

		ts, err := lpDecoder.Time(precision, time.Time{})
		if err != nil {
			if !r.config.EnablePartialWrites {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = fmt.Fprintf(w, "failed to parse timestamp for '%s' on line %d", measurement, line)
				return
			}
			batchDrops.AddDrop(FailedTimestamp, line, string(measurement))
			continue
		}

		if err = lpDecoder.Err(); err != nil {
			if !r.config.EnablePartialWrites {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = fmt.Fprintf(w, "failed to parse '%s' on line %d: %s", measurement, line, err.Error())
				return
			}
			batchDrops.AddDrop(FailedDecoder, line, string(measurement), err.Error())
			continue
		}

		err = batch.AddPoint(string(measurement), tags, fields, ts, common.InfluxMetricValueTypeUntyped)
		if err != nil {
			if !r.config.EnablePartialWrites {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = fmt.Fprintf(w, "failed to add '%s' on line %d to the batch: %s", measurement, line, err.Error())
				return
			}
			batchDrops.AddDrop(FailedBatchAdd, line, string(measurement), err.Error())
			continue
		}
	}

	err := r.nextConsumer.ConsumeMetrics(req.Context(), batch.GetMetrics())
	r.obsrecv.EndMetricsOp(ctx, dataFormat, batch.GetMetrics().DataPointCount(), err)
	if err != nil {
		if consumererror.IsPermanent(err) {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		r.logger.Debug("failed to pass metrics to next consumer: %s", err)
		return
	}

	if batchDrops.Count() > 0 {
		response := map[string]any{
			"error":   strings.Join(batchDrops.Errors(), ","),
			"dropped": batchDrops.Count(),
			"total":   line,
			"summary": batchDrops.Reasons(),
		}

		errorPrefix := "partial write"
		if batchDrops.Count() >= line {
			errorPrefix = "failed write"
		}

		if response["error"] != "" {
			response["error"] = errorPrefix + ": " + response["error"].(string)
		} else {
			response["error"] = errorPrefix
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(response)

		summary, _ := json.Marshal(batchDrops.Reasons())
		r.logger.Debug(fmt.Sprintf(
			"failed to add %d of %d lines to the batch %s: %s",
			batchDrops.Count(),
			line,
			string(summary),
			strings.Join(batchDrops.Errors(), ","),
		))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (r *metricsReceiver) handlePing(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
