CXX = g++
CXXFLAGS = -std=c++17 -O2 -I./include -I./parcer
SRCDIR = src
SOURCES = $(SRCDIR)/main.cpp $(SRCDIR)/utils.cpp $(SRCDIR)/query_evaluator.cpp $(SRCDIR)/btree_index.cpp $(SRCDIR)/collection.cpp
TARGET = no_sql_dbms

$(TARGET): $(SOURCES)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SOURCES)

clean:
	rm -f $(TARGET)

.PHONY: clean
