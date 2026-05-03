BUILD_DIR := build
OUTPUT := $(BUILD_DIR)/zep
IOMON := $(BUILD_DIR)/zpipe
ALERTCON := $(BUILD_DIR)/alertcon
LIBS := src/zep-common.lib.sh src/zep-stats.lib.sh src/zep-status.lib.sh src/zep-alerts.lib.sh src/zep-retention.lib.sh src/zep-transfer.lib.sh
MAIN := src/zeplicator

PREFIX ?= /usr/local
BINDIR := $(PREFIX)/bin

.PHONY: all clean install

all: $(IOMON) $(OUTPUT) $(ALERTCON)

install: all
	@echo "Installing to $(DESTDIR)$(BINDIR)..."
	mkdir -p $(DESTDIR)$(BINDIR)
	cp $(OUTPUT) $(DESTDIR)$(BINDIR)/zep
	cp $(IOMON) $(DESTDIR)$(BINDIR)/zpipe
	cp $(ALERTCON) $(DESTDIR)$(BINDIR)/alertcon
	chmod +x $(DESTDIR)$(BINDIR)/zep $(DESTDIR)$(BINDIR)/zpipe $(DESTDIR)$(BINDIR)/alertcon
	@echo "Installation complete."

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(IOMON): src/zpipe.c | $(BUILD_DIR)
	@echo "Compiling zpipe.c..."
	gcc -O3 $< -o $@

$(ALERTCON): src/alertcon | $(BUILD_DIR)
	@echo "Copying alertcon..."
	cp $< $@
	chmod +x $@

$(OUTPUT): $(LIBS) $(MAIN) | $(BUILD_DIR)
	@echo "Building $@"
	@echo "#!/bin/bash" > $@
	@echo "# zep - Compiled ZFS Replication Manager" >> $@
	@echo "# Built on: $$(date)" >> $@
	@echo "" >> $@
	@for lib in $(LIBS); do \
		echo "# --- BEGIN $$(basename $$lib) ---" >> $@; \
		grep -v "^#!" $$lib >> $@; \
		echo "# --- END $$(basename $$lib) ---" >> $@; \
		echo "" >> $@; \
	done
	@echo "# --- BEGIN zeplicator orchestrator ---" >> $@
	@grep -v "^#!" $(MAIN) | grep -v "^source " >> $@
	@echo "# --- END zeplicator orchestrator ---" >> $@
	@chmod +x $@
	@bash -n $@
	@echo "Done! Generated $@"
	@echo "Artifacts available in $(BUILD_DIR)/"

clean:
	rm -rf $(BUILD_DIR)
