// +build unit

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
	"strconv"
	"testing"

	"github.com/v3io/v3io-tsdb/pkg/config"

	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-go-http"
)

type testCrossLabelAggrSuite struct {
	suite.Suite
	logger logger.Logger
}

type mockItemUpdater struct{}

func (mip mockItemUpdater) UpdateItem(input *v3io.UpdateItemInput) {
	fmt.Printf("%s: %s\n", input.Path, *input.Expression)
}

func (suite *testCrossLabelAggrSuite) SetupSuite() {
	var err error

	suite.logger, err = nucliozap.NewNuclioZapTest("test")
	suite.Require().NoError(err)
}

func (suite *testCrossLabelAggrSuite) TestAppend() {

	conf := &config.V3ioConfig{TablePath: "TestCrossLabelAggrSuite", CrossLabelAggregateAppenderWorkers: 1}

	timeFunc := func(t int64) (int64, int) {
		partition := t / 100
		cell := int(t % 100)
		return partition, cell
	}

	aggrAppender := NewCrossLabelAggregateAppender(suite.logger, conf, mockItemUpdater{}, nil, timeFunc)

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			aggrAppender.Append(strconv.Itoa(i), nil, int64(j), float64(j))
		}
	}

	aggrAppender.Terminate()
	aggrAppender.AwaitTermination()
}

func TestCrossLabelAggrSuite(t *testing.T) {
	suite.Run(t, new(testCrossLabelAggrSuite))
}
