SUBMIT_TAR = lab2.tar.gz
SUBMISSION_SITE = "https://web.stanford.edu/class/cs240/labs/submission/"
GO := GOPATH="$(PWD)" go

# Recursive wildcard. Usage: $(call rwildcard,dir/,*.c)
rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))

LIB_FILES := $(call rwildcard,src/mapreduce/,*.go)

all: wordcount invertedindex pagerank

wordcount: $(call rwildcard,src/wordcount/,*.go) $(LIB_FILES)
	$(GO) build wordcount

invertedindex: $(call rwildcard,src/invertedindex/,*.go) $(LIB_FILES)
	$(GO) build invertedindex

pagerank: $(call rwildcard,src/pagerank/,*.go) $(LIB_FILES)
	$(GO) build -gcflags '-N -l' pagerank 

test:
	@GOPATH="$(PWD)" ./test.sh

$(SUBMIT_TAR): $(call rwild,src/,*)
	tar -czf $@ src/

submission: $(SUBMIT_TAR)
	@echo "Your submission file "$^" was successfully created."
	@echo "Submit it at $(SUBMISSION_SITE)."

clean:
	rm -f wordcount invertedindex pagerank $(SUBMIT_TAR)
	rm -f data/output/mr* data/output/diff*
