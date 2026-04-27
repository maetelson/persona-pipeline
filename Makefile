.PHONY: test-unit test-fixture test-smoke test-full test-changed validate-config validate-schema

PYTHON ?= python

test-unit:
	$(PYTHON) run/devtools/test_matrix.py test-unit

test-fixture:
	$(PYTHON) run/devtools/test_matrix.py test-fixture

test-smoke:
	$(PYTHON) run/devtools/test_matrix.py test-smoke

test-full:
	$(PYTHON) run/devtools/test_matrix.py test-full

test-changed:
	$(PYTHON) run/devtools/test_matrix.py test-changed

validate-config:
	$(PYTHON) run/devtools/validate_config.py

validate-schema:
	$(PYTHON) run/devtools/validate_schema.py
