// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package influxdbreceiver

import (
	"fmt"
	"strings"
)

type BatchDropReason string

const (
	FailedMeasurement BatchDropReason = "failed_to_parse_measurement_name"
	FailedTag         BatchDropReason = "failed_to_parse_tag"
	FailedField       BatchDropReason = "failed_to_parse_field"
	FailedTimestamp   BatchDropReason = "failed_to_parse_timestamp"
	FailedLine        BatchDropReason = "failed_to_parse_line"
	FailedDecoder     BatchDropReason = "failed_to_parse_decoder"
	FailedBatchAdd    BatchDropReason = "failed_to_add_to_batch"
)

type BatchDrops struct {
	errors  []string
	limit   int
	reasons map[BatchDropReason]int
}

func NewBatchDrops(limit int) *BatchDrops {
	return &BatchDrops{
		errors:  []string{},
		limit:   limit,
		reasons: make(map[BatchDropReason]int),
	}
}

func (bd *BatchDrops) AddDrop(dropReason BatchDropReason, line int, details ...string) {
	if _, exists := bd.reasons[dropReason]; !exists {
		bd.reasons[dropReason] = 0
	}

	bd.reasons[dropReason]++

	if len(bd.errors) < bd.limit {
		message := fmt.Sprintf("%s on line %d", dropReason, line)
		if len(details) > 0 {
			message += ": " + strings.Join(details, ", ")
		}
		bd.errors = append(bd.errors, message)
	}
}

func (bd *BatchDrops) Errors() []string {
	return bd.errors
}

func (bd *BatchDrops) Count() int {
	total := 0
	for _, count := range bd.reasons {
		total += count
	}
	return total
}

func (bd *BatchDrops) Reasons() map[BatchDropReason]int {
	return bd.reasons
}
