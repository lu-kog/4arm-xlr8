# Variables
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -Iinclude -g
LDFLAGS = 
OBJDIR = obj
SRCDIR = src
INCDIR = include
TARGET = app

# Source and Object Files
SRCS = $(SRCDIR)/Utilities.cpp $(SRCDIR)/Logger.cpp $(SRCDIR)/csv_parser.cpp $(SRCDIR)/Schema.cpp \
       $(SRCDIR)/DbString.cpp $(SRCDIR)/Meta.cpp $(SRCDIR)/Column.cpp \
       $(SRCDIR)/DataBlock.cpp $(SRCDIR)/QueryNodes.cpp $(SRCDIR)/Main.cpp
OBJS = $(SRCS:$(SRCDIR)/%.cpp=$(OBJDIR)/%.o)

# Target
all: $(TARGET)

# Link Objects to Create Executable
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

# Compile Source Files to Object Files
$(OBJDIR)/%.o: $(SRCDIR)/%.cpp $(INCDIR)/*.h | $(OBJDIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Create Directories
$(OBJDIR):
	mkdir -p $(OBJDIR)

# Clean Build Artifacts
clean:
	rm -rf $(OBJDIR) $(TARGET)

# Phony Targets
.PHONY: all clean
