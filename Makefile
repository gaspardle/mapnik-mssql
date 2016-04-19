# To use clang, run:  make CXX=clang++

CXXFLAGS = $(shell mapnik-config --cflags) -fPIC -DUNICODE -std=c++11 -w

LIBS = $(shell mapnik-config --libs --ldflags --dep-libs) -lodbc -L$(HOME)/local/lib/

SRC = $(wildcard mssql/*.cpp)

OBJ = $(SRC:.cpp=.o)

BIN = mssql.input

all : $(SRC) $(BIN)

$(BIN) : $(OBJ)
	$(CXX) -shared $(OBJ) $(LIBS) -o $@

.cpp.o :
	$(CXX) -c $(CXXFLAGS) $< -o $@

.PHONY : clean

clean:
	rm -f $(OBJ)
	rm -f $(BIN)

deploy : all
	cp mssql.input $(shell mapnik-config --input-plugins)

install: all deploy

uninstall:
	-rm $(shell mapnik-config --input-plugins)/mssql.input

test/run: $(BIN) test/main.cpp
	$(CXX) -o ./test/run test/main.cpp test/mssql.cpp $(CXXFLAGS) $(LIBS)

test: test/run
	./test/run
