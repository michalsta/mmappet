INCLUDE_FLAGS=-I../../src/mmappet/cpp
WARN_FLAGS=-Wall -Wextra -Wpedantic


all: reader_example writer_example

%: %.cpp ../../src/mmappet/cpp/mmappet/mmappet.h
	$(CXX) $(INCLUDE_FLAGS) $(WARN_FLAGS) -o $@ $< -std=c++17
