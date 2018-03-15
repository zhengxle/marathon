.SECONDARY:

bin/amm-2.11:
	mkdir -p bin
	curl -L https://github.com/lihaoyi/Ammonite/releases/download/1.0.5/2.11-1.0.5 -L -o $@.work
	chmod +x $@.work
	mv $@.work $@

target/%/logstash-conf/json/30-output.conf:
	mkdir -p target/$*/logstash-conf
	../elk/bin/target bundle "$(PWD)/target/$*/bundle" --target-path "$(PWD)/target/$*/logstash-conf"

target/%/scratch:
	mkdir -p $@

target/%/scratch/marathon-logs.json.ld: target/%/logstash-conf/json/30-output.conf | target/%/scratch
	bin/run-logstash $(PWD)/target/$*/logstash-conf/json
	mv target/$*/logstash-conf/output.json.ld $@

target/%/marathon-log-metrics.json: target/%/scratch/marathon-logs.json.ld | bin/amm-2.11
	JAVA_OPTS=-Xmx2g bin/amm-2.11 data-tool.sc target/$*/scratch/marathon-logs.json.ld  > $@.work
	mv $@.work $@

target/%/scratch/marathon-pods.json: | target/%/scratch
	bin/maybe-gzcat $$(find target/$*/bundle/*_master -name "*v2_groups.json*" | head -n 1) | jq 'recurse(.groups[]) | .pods[]' -c > $@.work
	mv $@.work $@

target/%/scratch/marathon-apps.json: | target/%/scratch
	bin/maybe-gzcat $$(find target/$*/bundle/*_master -name "*v2_groups.json*" | head -n 1) | jq 'recurse(.groups[]) | .apps[]' -c > $@.work
	mv $@.work $@

target/%/scratch/marathon-tasks.json: | target/%/scratch
	bin/maybe-gzcat $$(find target/$*/bundle/*_master -name "*v2_tasks.json*" | head -n 1) | jq '.tasks[]' -c > $@.work
	mv $@.work $@

target/%/scratch/mesos-tasks.json: | target/%/scratch
	bin/maybe-gzcat $$(find target/$*/bundle/*_master -name "*master_state.json*" | head -n 1) | jq -M -c '.frameworks | map([.tasks, .unreachable_tasks, .completed_tasks]) | flatten[]' > $@.work
	mv $@.work $@

target/%/scratch/mesos-agents.json: | target/%/scratch
	bin/maybe-gzcat $$(find target/$*/bundle/*_master -name "*master_slaves.json*" | head -n 1) | jq -M -c '.slaves[]' > $@.work
	mv $@.work $@

target/%/scratch/mesos-frameworks.json: | target/%/scratch
	bin/maybe-gzcat $$(find target/$*/bundle/*_master -name "*master_frameworks.json*" | head -n 1) | jq -M -c '[.frameworks[], .unregistered_frameworks[], .completed_frameworks[]][]' > $@.work
	mv $@.work $@

target/%/scratch/mesos-info.json: | target/%/scratch
	bin/maybe-gzcat $$(find target/$*/bundle/*_master -name "*master_state.json*" | head -n 1) | jq -M -c '{version: .version, cluster: .cluster, elected_time: .elected_time}' > $@.work
	mv $@.work $@

target/%/cluster-stats.json: target/%/scratch/marathon-pods.json target/%/scratch/marathon-apps.json target/%/scratch/marathon-tasks.json target/%/scratch/mesos-tasks.json target/%/scratch/mesos-agents.json target/%/scratch/mesos-frameworks.json target/%/scratch/mesos-info.json
	JAVA_OPTS=-Xmx2g bin/amm-2.11 cluster-state-stats.sc target/$*/scratch/  > $@.work
	mv $@.work $@
