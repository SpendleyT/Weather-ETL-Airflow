$(info $(SHELL))

PY = python3
VENV = venv
BIN=$(VENV)/BIN


#Check for Windows installation
ifeq ($(OS), Windows_NT)
	BIN=$(VENV)/Scripts
	PY=python
endif
