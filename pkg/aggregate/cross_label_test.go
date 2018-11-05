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
	"sort"
	"strconv"
	"testing"

	"github.com/v3io/v3io-tsdb/pkg/config"

	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-go-http"
)

var expected = []string{
	`TestCrossLabelAggrSuite/0/0.26923b3022651de: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[0]+0.000000e00`,
	`TestCrossLabelAggrSuite/0/0.26923b3022651de: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[1]+1.000000e00`,
	`TestCrossLabelAggrSuite/0/0.26923b3022651de: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[2]+2.000000e00`,
	`TestCrossLabelAggrSuite/0/0.2a1b32a757460405: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[0]+0.000000e00`,
	`TestCrossLabelAggrSuite/0/0.2a1b32a757460405: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[1]+1.000000e00`,
	`TestCrossLabelAggrSuite/0/0.2a1b32a757460405: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[2]+2.000000e00`,
	`TestCrossLabelAggrSuite/0/0.af29345201a3ce48: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[0]+0.000000e00`,
	`TestCrossLabelAggrSuite/0/0.af29345201a3ce48: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[1]+1.000000e00`,
	`TestCrossLabelAggrSuite/0/0.af29345201a3ce48: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[2]+2.000000e00`,
	`TestCrossLabelAggrSuite/0/1.26923b3022651de: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[0]+0.000000e00`,
	`TestCrossLabelAggrSuite/0/1.26923b3022651de: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[1]+1.000000e00`,
	`TestCrossLabelAggrSuite/0/1.26923b3022651de: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[2]+2.000000e00`,
	`TestCrossLabelAggrSuite/0/1.2a1b32a757460405: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[0]+0.000000e00`,
	`TestCrossLabelAggrSuite/0/1.2a1b32a757460405: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[1]+1.000000e00`,
	`TestCrossLabelAggrSuite/0/1.2a1b32a757460405: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[2]+2.000000e00`,
	`TestCrossLabelAggrSuite/0/1.af29345201a3ce48: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[0]+0.000000e00`,
	`TestCrossLabelAggrSuite/0/1.af29345201a3ce48: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[1]+1.000000e00`,
	`TestCrossLabelAggrSuite/0/1.af29345201a3ce48: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[2]+2.000000e00`,
	`TestCrossLabelAggrSuite/0/2.26923b3022651de: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[0]+0.000000e00`,
	`TestCrossLabelAggrSuite/0/2.26923b3022651de: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[1]+1.000000e00`,
	`TestCrossLabelAggrSuite/0/2.26923b3022651de: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[2]+2.000000e00`,
	`TestCrossLabelAggrSuite/0/2.2a1b32a757460405: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[0]+0.000000e00`,
	`TestCrossLabelAggrSuite/0/2.2a1b32a757460405: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[1]+1.000000e00`,
	`TestCrossLabelAggrSuite/0/2.2a1b32a757460405: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[2]+2.000000e00`,
	`TestCrossLabelAggrSuite/0/2.af29345201a3ce48: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[0]+0.000000e00`,
	`TestCrossLabelAggrSuite/0/2.af29345201a3ce48: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[1]+1.000000e00`,
	`TestCrossLabelAggrSuite/0/2.af29345201a3ce48: _v_count=if_not_exists(a,init_array(128,'double',0.0)); _v_count[cell]=_v_count[2]+2.000000e00`,
}

type testCrossLabelAggrSuite struct {
	suite.Suite
	logger logger.Logger
}

type mockItemUpdater struct {
	requestChan chan string
	result      []string
}

func (mip mockItemUpdater) UpdateItem(input *v3io.UpdateItemInput) {
	mip.requestChan <- fmt.Sprintf("%s: %s", input.Path, *input.Expression)
}

func (suite *testCrossLabelAggrSuite) SetupSuite() {
	var err error

	suite.logger, err = nucliozap.NewNuclioZapTest("test")
	suite.Require().NoError(err)
}

func (suite *testCrossLabelAggrSuite) TestAppend() {

	conf := &config.V3ioConfig{TablePath: "TestCrossLabelAggrSuite", CrossLabelAggregateAppenderWorkers: 8}

	timeFunc := func(t int64) (int64, int) {
		partition := t / 100
		cell := int(t % 100)
		return partition, cell
	}

	mip := mockItemUpdater{requestChan: make(chan string, 128)}
	completionChan := make(chan struct{})
	go func() {
		for request := range mip.requestChan {
			mip.result = append(mip.result, request)
		}
		close(completionChan)
	}()
	aggrAppender := NewCrossLabelAggregateAppender(suite.logger, conf, mip, []string{"label"}, timeFunc)

	for i := 0; i < 3; i++ {
		for labelValue := 0; labelValue < 3; labelValue++ {
			for j := 0; j < 3; j++ {
				labels := map[string]string{"label": string(labelValue), "ignoreThis": string(j)}
				aggrAppender.Append(strconv.Itoa(i), labels, int64(j), float64(j))
			}
		}
	}

	aggrAppender.Terminate()
	aggrAppender.AwaitTermination()

	close(mip.requestChan)

	<-completionChan

	sort.Strings(mip.result)

	suite.Equal(expected, mip.result)
}

func TestCrossLabelAggrSuite(t *testing.T) {
	suite.Run(t, new(testCrossLabelAggrSuite))
}
