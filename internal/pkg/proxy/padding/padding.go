package padding

import (
	"crypto/md5"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/xflash-panda/server-anytls/internal/pkg/util"
)

const CheckMark = -1

var defaultPaddingScheme = []byte(`stop=8
0=30-30
1=100-400
2=400-500,c,500-1000,c,500-1000,c,500-1000,c,500-1000
3=9-9,500-1000
4=500-1000
5=500-1000
6=500-1000
7=500-1000`)

type paddingRange struct {
	isCheck bool
	min     int64
	max     int64
}

type PaddingFactory struct {
	parsedRanges map[uint32][]paddingRange
	RawScheme    []byte
	Stop         uint32
	Md5          string
}

var DefaultPaddingFactory atomic.Pointer[PaddingFactory]

func init() {
	UpdatePaddingScheme(defaultPaddingScheme)
}

func UpdatePaddingScheme(rawScheme []byte) bool {
	if p := NewPaddingFactory(rawScheme); p != nil {
		DefaultPaddingFactory.Store(p)
		return true
	}
	return false
}

func NewPaddingFactory(rawScheme []byte) *PaddingFactory {
	p := &PaddingFactory{
		RawScheme: rawScheme,
		Md5:       fmt.Sprintf("%x", md5.Sum(rawScheme)),
	}
	scheme := util.StringMapFromBytes(rawScheme)
	if len(scheme) == 0 {
		return nil
	}
	if stop, err := strconv.Atoi(scheme["stop"]); err == nil {
		p.Stop = uint32(stop)
	} else {
		return nil
	}
	p.parsedRanges = make(map[uint32][]paddingRange)
	for key, val := range scheme {
		pkt, err := strconv.Atoi(key)
		if err != nil {
			continue
		}
		sRanges := strings.Split(val, ",")
		ranges := make([]paddingRange, 0, len(sRanges))
		for _, sRange := range sRanges {
			if sRange == "c" {
				ranges = append(ranges, paddingRange{isCheck: true})
				continue
			}
			sRangeMinMax := strings.Split(sRange, "-")
			if len(sRangeMinMax) == 2 {
				_min, err := strconv.ParseInt(sRangeMinMax[0], 10, 64)
				if err != nil {
					continue
				}
				_max, err := strconv.ParseInt(sRangeMinMax[1], 10, 64)
				if err != nil {
					continue
				}
				_min, _max = min(_min, _max), max(_min, _max)
				if _min <= 0 || _max <= 0 {
					continue
				}
				ranges = append(ranges, paddingRange{min: _min, max: _max})
			}
		}
		p.parsedRanges[uint32(pkt)] = ranges
	}
	return p
}

func (p *PaddingFactory) GenerateRecordPayloadSizes(pkt uint32) (pktSizes []int) {
	ranges, ok := p.parsedRanges[pkt]
	if !ok {
		return nil
	}
	pktSizes = make([]int, 0, len(ranges))
	for _, r := range ranges {
		if r.isCheck {
			pktSizes = append(pktSizes, CheckMark)
			continue
		}
		if r.min == r.max {
			pktSizes = append(pktSizes, int(r.min))
		} else {
			pktSizes = append(pktSizes, int(rand.Int64N(r.max-r.min)+r.min))
		}
	}
	return pktSizes
}
