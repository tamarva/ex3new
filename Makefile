CC=g++
CXX=g++
RANLIB=ranlib

LIBSRC=MapReduceFramework.cpp Barrier.cpp
LIBOBJ=$(LIBSRC:.cpp=.o)

INCS=-I.
CFLAGS = -Wall -std=c++11 -fpermissive  -g $(INCS) 
CXXFLAGS = -Wall -std=c++11 -fpermissive  -g $(INCS) 

OSMLIB = libMapReduceFramework.a
TARGETS = $(OSMLIB)

TAR=tar
TARFLAGS=-cvf
TARNAME=ex5.tar
TARSRCS=$(LIBSRC) Makefile README Barrier.h

all: $(TARGETS)

$(TARGETS): $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

clean:
	$(RM) $(TARGETS) $(OSMLIB) $(OBJ) $(LIBOBJ) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)
