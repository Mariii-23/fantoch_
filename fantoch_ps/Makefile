SIM_OUTPUT="A"

parse2:
	grep "n = " $(SIM_OUTPUT) | grep -v "region" | sort > SIM
	python3 parse_sim.py SIM
	rm SIM

parse1:
	rg "n = " $(SIM_OUTPUT) | rg -v region | sort > SIM
	python3 parse_sim.py SIM
	rm SIM

parse:
	rg -e "(n = |POOL_SIZE|CONFLICTS)" $(file) | rg -v region | rg -v empty > sim.out

clean:
	rm -f *.log .metrics_* *.execution_log
