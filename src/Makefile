plugin:
	cd mrapps/ && go build -buildmode=plugin wc.go
master:plugin
	go run main/mrmaster/mrmaster.go
worker1:plugin
	timeout 3;go run main/mrworker/mrworker.go mrapps/wc.so
worker2:plugin
	go run main/mrworker/mrworker.go mrapps/wc.so
