package kwdb

import (
	"bufio"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

func newFileDataSource(fileName string) targets.DataSource {
	br := load.GetBufferedReader(fileName)

	return &fileDataSource{scanner: bufio.NewScanner(br)}
}

type fileDataSource struct {
	scanner *bufio.Scanner
	headers *common.GeneratedDataHeaders
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

func nextComma(s string, start int) int {
	for i := start; i < len(s); i++ {
		if s[i] == ',' {
			return i
		}
	}
	return -1
}

func parseSmallPositiveInt(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func trimSpaceString(s string) string {
	left := 0
	right := len(s) - 1
	for left <= right {
		switch s[left] {
		case ' ', '\t', '\n', '\r':
			left++
		default:
			goto trimRight
		}
	}
	return ""

trimRight:
	for right >= left {
		switch s[right] {
		case ' ', '\t', '\n', '\r':
			right--
		default:
			return s[left : right+1]
		}
	}
	return ""
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}
	p := &point{}
	line := d.scanner.Text()
	p.sqlType = line[0]
	switch line[0] {
	case Insert:
		c0 := nextComma(line, 0)
		c1 := nextComma(line, c0+1)
		c2 := nextComma(line, c1+1)
		if c0 < 0 || c1 < 0 || c2 < 0 {
			panic(line)
		}
		p.device = line[c0+1 : c1]
		p.tag = p.device
		p.fieldCount = parseSmallPositiveInt(line[c1+1 : c2])
		p.sql = trimSpaceString(line[c2+1:])

	case CreateTemplateTable:
		c0 := nextComma(line, 0)
		c1 := nextComma(line, c0+1)
		if c0 < 0 || c1 < 0 {
			panic(line)
		}
		p.template = line[c0+1 : c1] //cpu
		// p.device = parts[2]   //host_0
		p.sql = line[c1+1:] //(column) tags (tagStr)
	case CreateTable:
		c0 := nextComma(line, 0)
		c1 := nextComma(line, c0+1)
		c2 := nextComma(line, c1+1)
		if c0 < 0 || c1 < 0 || c2 < 0 {
			panic(line)
		}
		p.template = line[c0+1 : c1] //cpu
		p.device = line[c1+1 : c2]   //host_0
		p.sql = line[c2+1:]          //tags (tagValue)
	default:
		panic(line)
	}
	return data.NewLoadedPoint(p)
}
