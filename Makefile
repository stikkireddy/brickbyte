lock:
	@cd integrations/destination-databricks-py && poetry lock

install:
	@cd integrations/destination-databricks-py && poetry install