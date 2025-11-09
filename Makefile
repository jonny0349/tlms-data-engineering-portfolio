.PHONY: run test lint fmt clean

run:
	python -m src.pipeline_sim.main

test:
	pytest -q

lint:
	flake8 src tests

fmt:
	python - <<'PY'
import os, pathlib
# lightweight formatter: strip trailing spaces
for p in pathlib.Path('src').rglob('*.py'):
    s=open(p,'r',encoding='utf-8').read()
    s='\n'.join([line.rstrip() for line in s.splitlines()])+'\n'
    open(p,'w',encoding='utf-8').write(s)
print('formatted')
PY

clean:
	rm -rf data/bronze data/silver data/gold .pytest_cache
