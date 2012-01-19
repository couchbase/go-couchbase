include $(GOROOT)/src/Make.inc

TARG=github.com/couchbaselabs/go-couchbase
GOFILES=pools.go vbmap.go client.go mc.go mc_constants.go

include $(GOROOT)/src/Make.pkg
