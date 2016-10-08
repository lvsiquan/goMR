# goMR


## no talk, show the code(word counter)

```
func filterFunc(line string) bool {
	return !strings.EqualFold(line, "no")
}

func mapper(line interface{}) []goMR.KV {
	s := strings.Split(line.(string), " ")
	var pairs []goMR.KV
	for _, es := range s {
		var pair goMR.KV
		pair.K = es
		pair.V = 1
		pairs = append(pairs, pair)
	}

	return pairs
}

func reducer(kv goMR.KV) goMR.KV {
	// fmt.Printf("key is %s, value is  %v \n", kv.K, kv.V)
	var pairs goMR.KV
	pairs.K = kv.K
	pairs.V = len(kv.V.([]interface{}))
	return pairs
}

func ender(kv goMR.KV) {
	fmt.Printf("key is %s, repeat  %v \n", kv.K, kv.V)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	gomr := goMR.New(40, 4)//First param is the number of mapper,second param is the number of reducer
	gomr.DataFile("/home/dev/code/test.txt").Filter(filterFunc).Map(mapper)
	gomr.Reduce(reducer)
	gomr.End(ender)
	time.Sleep(5000000000000)
}
```
