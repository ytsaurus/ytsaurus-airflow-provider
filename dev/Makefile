.PHONY: build clean run

build:
	pip install build
	mkdir -p ../dist
	python -m build ..
	cp ../dist/*.whl .
	find ../ytsaurus_airflow_provider/example_dags/*.py -type f -exec ln -f {} ./dags/ \;

clean:
	rm -rf ../dist
	rm -f *.whl

run: clean build
	docker compose up --build -d
