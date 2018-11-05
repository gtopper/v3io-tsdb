/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package aggregate

import (
	"fmt"
	"hash/fnv"
	"path"
	"strconv"

	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"

	"github.com/nuclio/logger"
	"github.com/v3io/v3io-go-http"
)

type Appender interface {
	append(t int64, v float64) error
}

type sample struct {
	metric          string
	labels          map[string]string
	hashWithoutName uint64
	t               int64
	v               float64
}

type CrossLabelAggregateAppender struct {
	logger          logger.Logger
	conf            *config.V3ioConfig
	itemUpdater     ItemUpdater
	labels          []string
	pendingChans    []chan sample
	timeToCell      func(int64) (int64, int)
	completionChans []chan struct{}
}

type ItemUpdater interface {
	UpdateItem(input *v3io.UpdateItemInput)
}

func NewCrossLabelAggregateAppender(logger logger.Logger, conf *config.V3ioConfig, itemUpdater ItemUpdater, labels []string, timeToCell func(int64) (int64, int)) *CrossLabelAggregateAppender {
	var completionChans []chan struct{}
	var pendingChans []chan sample
	for i := 0; i < conf.CrossLabelAggregateAppenderWorkers; i++ {
		completionChans = append(completionChans, make(chan struct{}, 0))
		pendingChan := make(chan sample, 128)
		pendingChans = append(pendingChans, pendingChan)
	}

	res := &CrossLabelAggregateAppender{
		logger:          logger.GetChild("CrossLabelAggregateAppender"),
		conf:            conf,
		itemUpdater:     itemUpdater,
		labels:          labels,
		pendingChans:    pendingChans,
		timeToCell:      timeToCell,
		completionChans: completionChans,
	}

	for i := 0; i < conf.CrossLabelAggregateAppenderWorkers; i++ {
		go res.consumePending(i)
	}

	return res
}

func (c *CrossLabelAggregateAppender) Append(metric string, labels map[string]string, t int64, v float64) {
	hasher := fnv.New64()
	for _, labelName := range c.labels {
		labelValue, ok := labels[labelName]
		if !ok {
			continue
		}
		hasher.Write([]byte(labelName))
		hasher.Write([]byte(labelValue))
	}
	hashWithoutName := hasher.Sum64()
	hasher.Write([]byte(metric)) // Never returns an error
	hashWithName := hasher.Sum64()
	shard := hashWithName % uint64(c.conf.CrossLabelAggregateAppenderWorkers)
	pendingChan := c.pendingChans[shard]
	pendingChan <- sample{
		metric:          metric,
		labels:          labels,
		hashWithoutName: hashWithoutName,
		t:               t,
		v:               v,
	}
}

func (c *CrossLabelAggregateAppender) Terminate() {
	for _, ch := range c.pendingChans {
		close(ch)
	}
}

func (c *CrossLabelAggregateAppender) AwaitTermination() {
	for _, ch := range c.completionChans {
		<-ch
	}
}

func (c *CrossLabelAggregateAppender) consumePending(index int) {
	completionChan := c.completionChans[index]
	pendingChan := c.pendingChans[index]
	for sampleInstance := range pendingChan {
		partition, cell := c.timeToCell(sampleInstance.t)
		objectName := fmt.Sprintf("%s.%x", sampleInstance.metric, sampleInstance.hashWithoutName)
		pathString := path.Join(c.conf.TablePath, strconv.FormatInt(partition, 10), objectName)
		valueString := utils.FloatToNormalizedScientificStr(sampleInstance.v)
		expr := fmt.Sprintf(`_v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[%d]+%s`, cell, valueString)
		c.itemUpdater.UpdateItem(&v3io.UpdateItemInput{
			Path:       pathString,
			Expression: &expr,
		})
	}
	close(completionChan)
}
