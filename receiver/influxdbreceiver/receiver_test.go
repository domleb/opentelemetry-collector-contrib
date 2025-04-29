// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package influxdbreceiver

import (
	"context"
	"testing"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxdb1 "github.com/influxdata/influxdb1-client/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver/internal/metadata"
)

func TestWriteLineProtocol_v2API(t *testing.T) {
	addr := testutil.GetAvailableLocalAddress(t)
	config := &Config{
		ServerConfig: confighttp.ServerConfig{
			Endpoint: addr,
		},
	}
	nextConsumer := new(mockConsumer)

	receiver, outerErr := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(metadata.Type), config, nextConsumer)
	require.NoError(t, outerErr)
	require.NotNil(t, receiver)

	require.NoError(t, receiver.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, receiver.Shutdown(context.Background())) })

	t.Run("influxdb-client-v1", func(t *testing.T) {
		nextConsumer.lastMetricsConsumed = pmetric.NewMetrics()

		client, err := influxdb1.NewHTTPClient(influxdb1.HTTPConfig{
			Addr:    "http://" + addr,
			Timeout: time.Second,
		})
		require.NoError(t, err)

		batchPoints, err := influxdb1.NewBatchPoints(influxdb1.BatchPointsConfig{Precision: "µs"})
		require.NoError(t, err)
		point, err := influxdb1.NewPoint("cpu_temp", map[string]string{"foo": "bar"}, map[string]any{"gauge": 87.332})
		require.NoError(t, err)
		batchPoints.AddPoint(point)
		err = client.Write(batchPoints)
		require.NoError(t, err)

		metrics := nextConsumer.lastMetricsConsumed
		if assert.NotNil(t, metrics) && assert.Positive(t, metrics.DataPointCount()) {
			assert.Equal(t, 1, metrics.MetricCount())
			metric := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
			assert.Equal(t, "cpu_temp", metric.Name())
			if assert.Equal(t, pmetric.MetricTypeGauge, metric.Type()) && assert.Equal(t, pmetric.NumberDataPointValueTypeDouble, metric.Gauge().DataPoints().At(0).ValueType()) {
				assert.InEpsilon(t, 87.332, metric.Gauge().DataPoints().At(0).DoubleValue(), 0.001)
			}
		}
	})

	t.Run("influxdb-client-v2", func(t *testing.T) {
		nextConsumer.lastMetricsConsumed = pmetric.NewMetrics()

		o := influxdb2.DefaultOptions()
		o.SetPrecision(time.Microsecond)
		client := influxdb2.NewClientWithOptions("http://"+addr, "", o)
		t.Cleanup(client.Close)

		err := client.WriteAPIBlocking("my-org", "my-bucket").WriteRecord(context.Background(), "cpu_temp,foo=bar gauge=87.332")
		require.NoError(t, err)

		metrics := nextConsumer.lastMetricsConsumed
		if assert.NotNil(t, metrics) && assert.Positive(t, metrics.DataPointCount()) {
			assert.Equal(t, 1, metrics.MetricCount())
			metric := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
			assert.Equal(t, "cpu_temp", metric.Name())
			if assert.Equal(t, pmetric.MetricTypeGauge, metric.Type()) && assert.Equal(t, pmetric.NumberDataPointValueTypeDouble, metric.Gauge().DataPoints().At(0).ValueType()) {
				assert.InEpsilon(t, 87.332, metric.Gauge().DataPoints().At(0).DoubleValue(), 0.001)
			}
		}
	})
}

func TestDisablePartialWrite(t *testing.T) {
	addr := testutil.GetAvailableLocalAddress(t)
	config := &Config{
		ServerConfig: confighttp.ServerConfig{
			Endpoint: addr,
		},
	}
	nextConsumer := new(mockConsumer)

	receiver, outerErr := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(metadata.Type), config, nextConsumer)
	require.NoError(t, outerErr)
	require.NotNil(t, receiver)

	require.NoError(t, receiver.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, receiver.Shutdown(context.Background())) })

	t.Run("valid-batch", func(t *testing.T) {
		nextConsumer.lastMetricsConsumed = pmetric.NewMetrics()

		client, err := influxdb1.NewHTTPClient(influxdb1.HTTPConfig{
			Addr:    "http://" + addr,
			Timeout: time.Second,
		})
		require.NoError(t, err)

		batchPoints, err := influxdb1.NewBatchPoints(influxdb1.BatchPointsConfig{Precision: "µs"})
		require.NoError(t, err)

		point1, err := influxdb1.NewPoint("cpu_temp", map[string]string{"foo": "bar"}, map[string]any{"gauge": 87.332})
		require.NoError(t, err)
		batchPoints.AddPoint(point1)

		point2, err := influxdb1.NewPoint("memory_usage", map[string]string{"foo": "baz"}, map[string]any{"gauge": 65.123})
		require.NoError(t, err)
		batchPoints.AddPoint(point2)

		err = client.Write(batchPoints)
		require.NoError(t, err)

		metrics := nextConsumer.lastMetricsConsumed
		assert.Equal(t, 2, metrics.MetricCount())
	})

	t.Run("batch-with-invalid-histogram", func(t *testing.T) {
		nextConsumer.lastMetricsConsumed = pmetric.NewMetrics()

		client, err := influxdb1.NewHTTPClient(influxdb1.HTTPConfig{
			Addr:    "http://" + addr,
			Timeout: time.Second,
		})
		require.NoError(t, err)

		batchPoints, err := influxdb1.NewBatchPoints(influxdb1.BatchPointsConfig{Precision: "µs"})
		require.NoError(t, err)

		invalidPoint, err := influxdb1.NewPoint("invalid_histogram", map[string]string{"foo": "quux"}, map[string]any{
			"bucket_0_10":  5,
			"bucket_10_20": 15,
			"bucket_20_30": 10,
			"bucket_inf":   0,
			"count":        int64(30),
			"sum":          float64(450.0),
		})
		require.NoError(t, err)
		batchPoints.AddPoint(invalidPoint)

		validPoint, err := influxdb1.NewPoint("valid_histogram", map[string]string{"foo": "quux"}, map[string]any{
			"bucket_0_10":  5,
			"bucket_10_20": 15,
			"bucket_20_30": 10,
			"bucket_inf":   0,
			"count":        float64(30),
			"sum":          float64(450.0),
		})
		require.NoError(t, err)
		batchPoints.AddPoint(validPoint)

		err = client.Write(batchPoints)
		require.Error(t, err)

		metrics := nextConsumer.lastMetricsConsumed
		assert.Zero(t, metrics.MetricCount())

	})
}

func TestEnablePartialWrites(t *testing.T) {
	addr := testutil.GetAvailableLocalAddress(t)
	config := &Config{
		ServerConfig: confighttp.ServerConfig{
			Endpoint: addr,
		},
		EnablePartialWrites: true,
	}
	nextConsumer := new(mockConsumer)

	receiver, outerErr := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(metadata.Type), config, nextConsumer)
	require.NoError(t, outerErr)
	require.NotNil(t, receiver)

	require.NoError(t, receiver.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, receiver.Shutdown(context.Background())) })

	t.Run("valid-batch", func(t *testing.T) {
		nextConsumer.lastMetricsConsumed = pmetric.NewMetrics()

		client, err := influxdb1.NewHTTPClient(influxdb1.HTTPConfig{
			Addr:    "http://" + addr,
			Timeout: time.Second,
		})
		require.NoError(t, err)

		batchPoints, err := influxdb1.NewBatchPoints(influxdb1.BatchPointsConfig{Precision: "µs"})
		require.NoError(t, err)

		point1, err := influxdb1.NewPoint("cpu_temp", map[string]string{"foo": "bar"}, map[string]any{"gauge": 87.332})
		require.NoError(t, err)
		batchPoints.AddPoint(point1)

		point2, err := influxdb1.NewPoint("memory_usage", map[string]string{"foo": "baz"}, map[string]any{"gauge": 65.123})
		require.NoError(t, err)
		batchPoints.AddPoint(point2)

		err = client.Write(batchPoints)
		require.NoError(t, err)

		metrics := nextConsumer.lastMetricsConsumed
		assert.Equal(t, 2, metrics.MetricCount())
	})

	t.Run("batch-with-invalid-histogram", func(t *testing.T) {
		nextConsumer.lastMetricsConsumed = pmetric.NewMetrics()

		client, err := influxdb1.NewHTTPClient(influxdb1.HTTPConfig{
			Addr:    "http://" + addr,
			Timeout: time.Second,
		})
		require.NoError(t, err)

		batchPoints, err := influxdb1.NewBatchPoints(influxdb1.BatchPointsConfig{Precision: "µs"})
		require.NoError(t, err)

		invalidPoint, err := influxdb1.NewPoint("invalid_histogram", map[string]string{"foo": "quux"}, map[string]any{
			"bucket_0_10":  5,
			"bucket_10_20": 15,
			"bucket_20_30": 10,
			"bucket_inf":   0,
			"count":        int64(30),
			"sum":          float64(450.0),
		})
		require.NoError(t, err)
		batchPoints.AddPoint(invalidPoint)

		validPoint, err := influxdb1.NewPoint("valid_histogram", map[string]string{"foo": "quux"}, map[string]any{
			"bucket_0_10":  5,
			"bucket_10_20": 15,
			"bucket_20_30": 10,
			"bucket_inf":   0,
			"count":        float64(30),
			"sum":          float64(450.0),
		})
		require.NoError(t, err)
		batchPoints.AddPoint(validPoint)

		err = client.Write(batchPoints)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "partial write")

		metrics := nextConsumer.lastMetricsConsumed
		assert.Equal(t, metrics.MetricCount(), 1)

	})
}

func TestEnableTrackingErrors(t *testing.T) {
	addr := testutil.GetAvailableLocalAddress(t)
	config := &Config{
		ServerConfig: confighttp.ServerConfig{
			Endpoint: addr,
		},
		EnablePartialWrites: true,
		MaxTrackedErrors:    1,
	}
	nextConsumer := new(mockConsumer)

	receiver, outerErr := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(metadata.Type), config, nextConsumer)
	require.NoError(t, outerErr)
	require.NotNil(t, receiver)

	require.NoError(t, receiver.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, receiver.Shutdown(context.Background())) })

	t.Run("batch-with-multiple-invalid-histograms", func(t *testing.T) {
		nextConsumer.lastMetricsConsumed = pmetric.NewMetrics()

		client, err := influxdb1.NewHTTPClient(influxdb1.HTTPConfig{
			Addr:    "http://" + addr,
			Timeout: time.Second,
		})
		require.NoError(t, err)

		batchPoints, err := influxdb1.NewBatchPoints(influxdb1.BatchPointsConfig{Precision: "µs"})
		require.NoError(t, err)

		invalidPoint, err := influxdb1.NewPoint("first_invalid_histogram", map[string]string{"foo": "quux"}, map[string]any{
			"bucket_0_10":  5,
			"bucket_10_20": 15,
			"bucket_20_30": 10,
			"bucket_inf":   0,
			"count":        int64(30),
			"sum":          float64(450.0),
		})
		require.NoError(t, err)
		batchPoints.AddPoint(invalidPoint)

		anohterInvalidPoint, err := influxdb1.NewPoint("second_invalid_histogram", map[string]string{"foo": "quux"}, map[string]any{
			"bucket_0_10":  5,
			"bucket_10_20": 15,
			"bucket_20_30": 10,
			"bucket_inf":   0,
			"count":        int64(30),
			"sum":          float64(450.0),
		})
		require.NoError(t, err)
		batchPoints.AddPoint(anohterInvalidPoint)

		validPoint, err := influxdb1.NewPoint("valid_histogram", map[string]string{"foo": "quux"}, map[string]any{
			"bucket_0_10":  5,
			"bucket_10_20": 15,
			"bucket_20_30": 10,
			"bucket_inf":   0,
			"count":        float64(30),
			"sum":          float64(450.0),
		})
		require.NoError(t, err)
		batchPoints.AddPoint(validPoint)

		err = client.Write(batchPoints)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "partial write")
		assert.Contains(t, err.Error(), "first_invalid_histogram")
		assert.NotContains(t, err.Error(), "second_invalid_histogram")

		metrics := nextConsumer.lastMetricsConsumed
		assert.Equal(t, metrics.MetricCount(), 1)

	})
}

type mockConsumer struct {
	lastMetricsConsumed pmetric.Metrics
}

func (m *mockConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (m *mockConsumer) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	m.lastMetricsConsumed = pmetric.NewMetrics()
	md.CopyTo(m.lastMetricsConsumed)
	return nil
}
