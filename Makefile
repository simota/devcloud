# devcloud Makefile
#
# Convenience wrapper for the commands documented in AGENTS.md and README.md.
# Run `make help` to see all available targets.

SHELL          := /bin/bash
.DEFAULT_GOAL  := help

# ---- Tunables ----------------------------------------------------------------
CARGO          ?= cargo
NPM            ?= npm
BIN_DIR        ?= bin
BIN            ?= $(BIN_DIR)/devcloud
ORCH_PKG       ?= devcloud-orchestrator
DASHBOARD_DIR  ?= web/dashboard
SERVICES       ?= mail s3 gcs dynamodb bigquery sqs pubsub redshift
VERIFY_STAGE   ?= full

.PHONY: help

help: ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} \
	     /^[a-zA-Z0-9_.-]+:.*?## / {printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf "\nPer-service targets (expand %% with: $(SERVICES)):\n"
	@printf "  \033[36m%-26s\033[0m %s\n" "e2e-<svc>"    "Run scripts/<svc>-e2e.sh"
	@printf "  \033[36m%-26s\033[0m %s\n" "verify-<svc>" "Run scripts/<svc>-autoloop/verify.sh (VERIFY_STAGE=$(VERIFY_STAGE))"

# ---- Rust: build & run -------------------------------------------------------
.PHONY: build run init up dashboard reset fmt check test

build: ## Build the devcloud CLI binary into $(BIN).
	@mkdir -p $(BIN_DIR)
	$(CARGO) build -p $(ORCH_PKG)
	cp target/debug/devcloud $(BIN)

run: build ## Build then run `devcloud up` from the built binary.
	./$(BIN) up

init: ## Write .devcloud/config.yaml with default ports/services.
	$(CARGO) run -p $(ORCH_PKG) -- init

up: ## Start all enabled services + dashboard (Ctrl-C to stop).
	$(CARGO) run -p $(ORCH_PKG) -- up

dashboard: ## Start only the dashboard server.
	$(CARGO) run -p $(ORCH_PKG) -- dashboard

reset: ## Wipe .devcloud/data for the configured workspace.
	$(CARGO) run -p $(ORCH_PKG) -- reset

fmt: ## Format the Rust workspace.
	$(CARGO) fmt --all

check: ## Check the Rust workspace.
	$(CARGO) check --workspace

test: ## Run Rust workspace tests.
	$(CARGO) test --workspace

# ---- Dashboard (React / Vite) ------------------------------------------------
.PHONY: dashboard-install dashboard-dev dashboard-build dashboard-typecheck

dashboard-install: ## npm install in web/dashboard.
	cd $(DASHBOARD_DIR) && $(NPM) install

dashboard-dev: ## Vite dev server (proxies /api to a running devcloud up).
	cd $(DASHBOARD_DIR) && $(NPM) run dev

dashboard-build: ## Typecheck + emit static assets into services/dashboard/assets/react.
	cd $(DASHBOARD_DIR) && $(NPM) run build

dashboard-typecheck: ## TypeScript check only.
	cd $(DASHBOARD_DIR) && $(NPM) run typecheck

# ---- E2E smoke ---------------------------------------------------------------
# `make e2e-s3` etc. — boot devcloud up, exercise the service, tear down.
.PHONY: $(addprefix e2e-,$(SERVICES)) e2e-all

$(addprefix e2e-,$(SERVICES)): e2e-%: ## Run the E2E smoke script for service %.
	bash scripts/$*-e2e.sh

e2e-all: ## Run every per-service E2E smoke script sequentially.
	@for svc in $(SERVICES); do \
		echo "==> e2e: $$svc"; \
		bash scripts/$$svc-e2e.sh || exit $$?; \
	done

# ---- Acceptance gates (per-service autoloops) --------------------------------
# `make verify-s3` runs scripts/s3-autoloop/verify.sh at VERIFY_STAGE=$(VERIFY_STAGE).
# Pub/Sub and Redshift only ship verify scripts under their primary autoloop folder.
VERIFY_SERVICES ?= mail s3 gcs dynamodb bigquery sqs pubsub
.PHONY: $(addprefix verify-,$(VERIFY_SERVICES)) verify-all

$(addprefix verify-,$(VERIFY_SERVICES)): verify-%: ## Run the autoloop verify gate for service %.
	VERIFY_STAGE=$(VERIFY_STAGE) bash scripts/$*-autoloop/verify.sh

verify-all: ## Run every per-service acceptance gate sequentially.
	@for svc in $(VERIFY_SERVICES); do \
		echo "==> verify($(VERIFY_STAGE)): $$svc"; \
		VERIFY_STAGE=$(VERIFY_STAGE) bash scripts/$$svc-autoloop/verify.sh || exit $$?; \
	done

# ---- Housekeeping ------------------------------------------------------------
.PHONY: clean clean-data clean-dashboard

clean: clean-data ## Remove build artifacts and runtime data.
	rm -rf $(BIN_DIR)

clean-data: ## Remove .devcloud/data (runtime storage).
	rm -rf .devcloud/data

clean-dashboard: ## Remove dashboard node_modules and built bundle.
	rm -rf $(DASHBOARD_DIR)/node_modules services/dashboard/assets/react
