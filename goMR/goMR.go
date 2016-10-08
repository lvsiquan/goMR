package goMR

import (
	"bufio"
	"hash/crc32"
	"os"
	"strings"
	"time"
)

const endflag = "EOFEOFEOF"

type goMRContext struct {
	File              string
	MapperSize        uint32
	ReducerSize       uint32
	mapperInputChans  []chan interface{}
	reducerInputChans []chan KV
	shuffleChan       chan KV
	endChan           chan KV
}

type KV struct {
	K interface{}
	V interface{}
}

func New(mapperSize uint32, reducerSize uint32) (ctx *goMRContext) {
	ctx = &goMRContext{}
	if mapperSize <= 0 {
		ctx.MapperSize = 1
	} else {
		ctx.MapperSize = mapperSize
	}
	if reducerSize <= 0 {
		ctx.ReducerSize = 1
	} else {
		ctx.ReducerSize = reducerSize
	}

	ctx.mapperInputChans = make([]chan interface{}, ctx.MapperSize, ctx.MapperSize)
	ctx.reducerInputChans = make([]chan KV, ctx.ReducerSize, ctx.ReducerSize)

	var i uint32
	for i = 0; i < ctx.MapperSize; i++ {
		ctx.mapperInputChans[i] = make(chan interface{})
	}

	for i = 0; i < ctx.ReducerSize; i++ {
		ctx.reducerInputChans[i] = make(chan KV)
	}

	ctx.shuffleChan = make(chan KV, 3)
	ctx.endChan = make(chan KV, 20)
	return
}

func readFile(file string, output chan string) {
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	buf := bufio.NewScanner(f)
	buf.Split(bufio.ScanLines)
	for buf.Scan() {
		line := buf.Text()
		// fmt.Println("line is ::: " + line)
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		output <- line
	}
	output <- endflag

	defer close(output)
}

func (ctx *goMRContext) DataFile(filepath string) *goMRContext {
	ctx.File = filepath
	return ctx
}

func (ctx *goMRContext) Filter(handler func(v string) bool) *goMRContext {
	lineChan := make(chan string)
	go readFile(ctx.File, lineChan)

	go func() {
		var i uint32 = 0
		for line := range lineChan {
			if handler(line) {
				_i := i % ctx.MapperSize
				// fmt.Printf("send msg to mapper : %s, index is : %d \n", line, _i)
				i++
				ctx.mapperInputChans[_i] <- line
			}
		}
	}()

	return ctx
}

func (ctx *goMRContext) Map(mapper func(input interface{}) []KV) *goMRContext {
	go ctx.shuffle()
	for _, c := range ctx.mapperInputChans {
		go func(ch chan interface{}) {
			for b := range ch {
				if strings.EqualFold(b.(string), endflag) {
					time.Sleep(200)
				}
				for _, each := range mapper(b) {
					ctx.shuffleChan <- each

				}
			}
		}(c)
	}
	return ctx
}

func (ctx *goMRContext) shuffle() {
	vmap := make(map[interface{}][]interface{})
	for v := range ctx.shuffleChan {
		// fmt.Printf("shuffle key : %s \n", v.K)
		if v.K == endflag {
			break
		}
		vmap[v.K] = append(vmap[v.K], v.V)
	}

	for k, v := range vmap {
		_i := crc32.ChecksumIEEE([]byte(k.(string))) % ctx.ReducerSize
		var reduceKV KV
		reduceKV.K = k
		reduceKV.V = v
		ctx.reducerInputChans[_i] <- reduceKV
	}
}

func (ctx *goMRContext) Reduce(reducer func(input KV) KV) *goMRContext {
	for _, c := range ctx.reducerInputChans {
		go func(ch chan KV) {
			for b := range ch {
				ctx.endChan <- reducer(b)
			}
		}(c)
	}
	return ctx
}

func (ctx *goMRContext) End(ender func(input KV)) {
	go func() {
		for kv := range ctx.endChan {
			ender(kv)
		}
	}()
}
