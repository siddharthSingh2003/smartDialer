db:       ; docker compose up -d && sleep 3 && $(MAKE) migrate
migrate:  ; docker compose exec -T db psql -U dialer -d dialer -f - < migrations/001_init.sql
seed:     ; python -m smartdialer.cli seed --agents 100 --borrowers 5000
run:      ; python -m smartdialer.cli worker --id w1
api:      ; python -m smartdialer.cli api
sim:      ; python -m smartdialer.cli sim --scenario all --out loadtest/results
test:     ; pytest -q
load:     ; python loadtest/run_load.py --agents 1000
down:     ; docker compose down
