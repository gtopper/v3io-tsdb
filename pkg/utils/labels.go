// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
)

const sep = '\xff'

// Well-known label names used by Prometheus components.
const (
	MetricName   = "__name__"
	AlertName    = "alertname"
	BucketLabel  = "le"
	InstanceName = "instance"
)

// Label is a key/value pair of strings.
type Label struct {
	Name, Value string
}

// Labels is a sorted set of labels. Order has to be guaranteed upon
// instantiation.
type Labels []Label

type LabelsIfc interface {
	GetKey() (string, string)
	GetExpr() string
	Filter([]string) LabelsIfc
	LabelNames() []string
	Hash() uint64
	HashWithName() uint64
}

func (ls Labels) Filter(keep []string) LabelsIfc {
	var res Labels
	for _, l := range ls {
		if l.Name == MetricName {
			res = append(res, l)
			continue
		}
		for _, keepLabel := range keep {
			if l.Name == keepLabel {
				res = append(res, l)
				break
			}
		}
	}
	return res
}

// convert Label set to a string in the form key1=v1,key2=v2.. + name
func (ls Labels) GetKey() (string, string) {
	var keyBuilder strings.Builder
	name := ""
	for _, lbl := range ls {
		if lbl.Name == MetricName {
			name = lbl.Value
		} else {
			keyBuilder.WriteString(lbl.Name)
			keyBuilder.WriteString("=")
			keyBuilder.WriteString(lbl.Value)
			keyBuilder.WriteString(",")
		}
	}
	if keyBuilder.Len() == 0 {
		return name, ""
	}

	// Discard last comma
	key := keyBuilder.String()[:keyBuilder.Len()-1]

	return name, key

}

// create update expression
func (ls Labels) GetExpr() string {
	var lblExprBuilder strings.Builder
	for _, lbl := range ls {
		if lbl.Name != MetricName {
			fmt.Fprintf(&lblExprBuilder, "%s='%s'; ", lbl.Name, lbl.Value)
		} else {
			fmt.Fprintf(&lblExprBuilder, "_name='%s'; ", lbl.Value)
		}
	}

	return lblExprBuilder.String()
}

func (ls Labels) LabelNames() []string {
	res := make([]string, len(ls))
	for i, l := range ls {
		res[i] = l.Name
	}
	return res
}

func (ls Labels) Len() int           { return len(ls) }
func (ls Labels) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }
func (ls Labels) Less(i, j int) bool { return ls[i].Name < ls[j].Name }

func (ls Labels) String() string {
	var b bytes.Buffer

	b.WriteByte('{')
	for i, l := range ls {
		if i > 0 {
			b.WriteByte(',')
			b.WriteByte(' ')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(l.Value))
	}
	b.WriteByte('}')

	return b.String()
}

// MarshalJSON implements json.Marshaler.
func (ls Labels) MarshalJSON() ([]byte, error) {
	return json.Marshal(ls.Map())
}

// UnmarshalJSON implements json.Unmarshaler.
func (ls *Labels) UnmarshalJSON(b []byte) error {
	var m map[string]string

	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	*ls = LabelsFromMap(m)
	return nil
}

// Hash returns a hash value for the label set.
func (ls Labels) HashWithMetricName() (uint64, error) {
	b := make([]byte, 0, 1024)

	for _, v := range ls {
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}

	hash := xxhash.New()
	_, err := hash.Write(b)
	if err != nil {
		return 0, err
	}
	return hash.Sum64(), nil
}

// Hash returns a hash value for the label set.
func (ls Labels) Hash() uint64 {
	b := make([]byte, 0, 1024)

	for _, v := range ls {
		if v.Name == MetricName {
			continue
		}
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}

	hash := xxhash.New()
	_, err := hash.Write(b)
	if err != nil {
		return 0
	}
	return hash.Sum64()
}

func (ls Labels) HashWithName() uint64 {
	b := make([]byte, 0, 1024)

	for _, v := range ls {
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}

	hash := xxhash.New()
	_, err := hash.Write(b)
	if err != nil {
		return 0
	}
	return hash.Sum64()
}

// Copy returns a copy of the labels.
func (ls Labels) Copy() Labels {
	res := make(Labels, len(ls))
	copy(res, ls)
	return res
}

// Get returns the value for the label with the given name.
// Returns an empty string if the label doesn't exist.
func (ls Labels) Get(name string) string {
	for _, l := range ls {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// Has returns true if the label with the given name is present.
func (ls Labels) Has(name string) bool {
	for _, l := range ls {
		if l.Name == name {
			return true
		}
	}
	return false
}

// Equal returns whether the two label sets are equal.
func Equal(ls, o Labels) bool {
	if len(ls) != len(o) {
		return false
	}
	for i, l := range ls {
		if l.Name != o[i].Name || l.Value != o[i].Value {
			return false
		}
	}
	return true
}

// Map returns a string map of the labels.
func (ls Labels) Map() map[string]interface{} {
	m := make(map[string]interface{}, len(ls))
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}

// ToLabels returns a sorted Labels from the given labels.
// The caller has to guarantee that all label names are unique.
func ToLabels(ls ...Label) Labels {
	set := make(Labels, 0, len(ls))
	for _, l := range ls {
		set = append(set, l)
	}
	sort.Sort(set)

	return set
}

// LabelsFromMap returns new sorted Labels from the given map.
func LabelsFromMap(m map[string]string) Labels {
	l := make([]Label, 0, len(m))
	for k, v := range m {
		l = append(l, Label{Name: k, Value: v})
	}
	return ToLabels(l...)
}

// LabelsFromStringList creates new labels from pairs of strings.
func LabelsFromStringList(ss ...string) Labels {
	if len(ss)%2 != 0 {
		panic("invalid number of strings")
	}
	var res Labels
	for i := 0; i < len(ss); i += 2 {
		res = append(res, Label{Name: ss[i], Value: ss[i+1]})
	}

	sort.Sort(res)
	return res
}

// LabelsFromStringList creates new labels from a string in the following format key1=label1[,key2=label2,...]
func LabelsFromStringWithName(name, lbls string) (Labels, error) {

	if err := IsValidMetricName(name); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Illegal metric name: '%s'", name))
	}

	lset := Labels{Label{Name: config.PrometheusMetricNameAttribute, Value: name}}

	moreLabels, err := LabelsFromString(lbls)
	if err != nil {
		return nil, err
	}
	lset = append(lset, moreLabels...)
	sort.Sort(lset)
	return lset, nil
}

func LabelsFromString(lbls string) (Labels, error) {
	lset := Labels{}

	if lbls != "" {
		splitLset := strings.Split(lbls, ",")
		for _, l := range splitLset {
			splitLbl := strings.Split(l, "=")
			if len(splitLbl) != 2 {
				return nil, errors.New("labels must be in the form 'key1=label1[,key2=label2,...]'")
			}

			if err := IsValidLabelName(splitLbl[0]); err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("Illegal label name: '%s'", splitLbl[0]))
			}
			lset = append(lset, Label{Name: splitLbl[0], Value: splitLbl[1]})
		}
	}
	sort.Sort(lset)
	return lset, nil
}

// Compare compares the two label sets.
// The result will be 0 if a==b, <0 if a < b, and >0 if a > b.
func Compare(a, b Labels) int {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}

	for i := 0; i < l; i++ {
		if d := strings.Compare(a[i].Name, b[i].Name); d != 0 {
			return d
		}
		if d := strings.Compare(a[i].Value, b[i].Value); d != 0 {
			return d
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a) - len(b)
}

// Builder allows modifiying Labels.
type Builder struct {
	base Labels
	del  []string
	add  []Label
}

// NewBuilder returns a new LabelsBuilder
func NewBuilder(base Labels) *Builder {
	return &Builder{
		base: base,
		del:  make([]string, 0, 5),
		add:  make([]Label, 0, 5),
	}
}

// Del deletes the label of the given name.
func (b *Builder) Del(ns ...string) *Builder {
	for _, n := range ns {
		for i, a := range b.add {
			if a.Name == n {
				b.add = append(b.add[:i], b.add[i+1:]...)
			}
		}
		b.del = append(b.del, n)
	}
	return b
}

// Set the name/value pair as a label.
func (b *Builder) Set(n, v string) *Builder {
	for i, a := range b.add {
		if a.Name == n {
			b.add[i].Value = v
			return b
		}
	}
	b.add = append(b.add, Label{Name: n, Value: v})

	return b
}

// Labels returns the labels from the builder. If no modifications
// were made, the original labels are returned.
func (b *Builder) Labels() Labels {
	if len(b.del) == 0 && len(b.add) == 0 {
		return b.base
	}

	// In the general case, labels are removed, modified or moved
	// rather than added.
	res := make(Labels, 0, len(b.base))
Outer:
	for _, l := range b.base {
		for _, n := range b.del {
			if l.Name == n {
				continue Outer
			}
		}
		for _, la := range b.add {
			if l.Name == la.Name {
				continue Outer
			}
		}
		res = append(res, l)
	}
	res = append(res, b.add...)
	sort.Sort(res)

	return res
}
