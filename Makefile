build:
	mkdir -p bin && scala-cli --power package ./ -o ./bin/fdbscan -f
