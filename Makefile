VENV_BIN ?= python3 -m venv
VENV_DIR ?= .venv
PIP_CMD ?= pip3

USERNAME ?= admin123
DB_NAME ?= testdb
USERPWD ?= mysecretpassword
STACK_NAME ?= DMsSampleSetupStack
ENDPOINT_URL = http://localhost.localstack.cloud:4566
export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test
export AWS_DEFAULT_REGION ?= us-east-1

VENV_RUN = . $(VENV_ACTIVATE)

CLOUD_ENV = USERNAME=$(USERNAME) DB_NAME=$(DB_NAME) USERPWD=$(USERPWD) STACK_NAME=$(STACK_NAME)
LOCAL_ENV = USERNAME=$(USERNAME) DB_NAME=$(DB_NAME) USERPWD=$(USERPWD) STACK_NAME=$(STACK_NAME) ENDPOINT_URL=$(ENDPOINT_URL)

ifeq ($(OS), Windows_NT)
	VENV_ACTIVATE = $(VENV_DIR)/Scripts/activate
else
	VENV_ACTIVATE = $(VENV_DIR)/bin/activate
endif

usage:                    ## Show this help
	@grep -Fh "##" $(MAKEFILE_LIST) | grep -Fv fgrep | sed -e 's/:.*##\s*/##/g' | awk -F'##' '{ printf "%-25s %s\n", $$1, $$2 }'

$(VENV_ACTIVATE):
	test -d $(VENV_DIR) || $(VENV_BIN) $(VENV_DIR)
	$(VENV_RUN); touch $(VENV_ACTIVATE)

venv: $(VENV_ACTIVATE)    ## Create a new (empty) virtual environment

start:		## Start LocalStack
	@test -n "${LOCALSTACK_AUTH_TOKEN}" || (echo "LOCALSTACK_AUTH_TOKEN is not set. Find your token at https://app.localstack.cloud/workspace/auth-token"; exit 1)
	$(LOCAL_ENV) LOCALSTACK_AUTH_TOKEN=$(LOCALSTACK_AUTH_TOKEN) docker compose up --build --detach --wait

stop:		## Stop LocalStack
	docker compose down

ready:		## Wait until LocalStack is ready
	@echo Waiting on the LocalStack container...
	@localstack wait -t 30 && echo LocalStack is ready to use! || (echo Gave up waiting on LocalStack, exiting. && exit 1)

logs:		## Save the logs in a separate file
	@localstack logs > logs.txt

install: venv
	$(VENV_RUN); $(PIP_CMD) install -r requirements.txt

deploy:
	$(VENV_RUN); $(LOCAL_ENV) cdklocal bootstrap --output ./cdk.local.out
	$(VENV_RUN); $(LOCAL_ENV) cdklocal deploy --require-approval never --output ./cdk.local.out

deploy-aws:
	$(VENV_RUN); $(CLOUD_ENV) cdk bootstrap
	$(VENV_RUN); $(CLOUD_ENV) cdk deploy --require-approval never

destroy:
	docker-compose down

destroy-aws: venv
	$(VENV_RUN); $(CLOUD_ENV) cdk destroy --require-approval never

run:
	$(VENV_RUN); $(LOCAL_ENV) python run.py

run-aws:
	$(VENV_RUN); $(CLOUD_ENV) python run.py

.PHONY: usage venv start stop ready logs install deploy deploy-aws destroy destroy-aws run run-aws
