# This is the Makefile helping you submit the labs.
# Just create 6.5840/api.key with your API key in it,
# and submit your lab with the following command:
#     $ make [lab1|lab2a|lab2b|lab2c|lab2d|lab3a|lab3b|lab4a|lab4b]

LABS=" lab1 lab2a lab2b lab2c lab2d lab3a lab3b lab4a lab4b "

%: check-%
	@echo "Preparing $@-handin.tar.gz"
	@if echo $(LABS) | grep -q " $@ " ; then \
		echo "Tarring up your submission..." ; \
		COPYFILE_DISABLE=1 tar cvzf $@-handin.tar.gz \
			"--exclude=src/main/pg-*.txt" \
			"--exclude=src/main/diskvd" \
			"--exclude=src/mapreduce/824-mrinput-*.txt" \
			"--exclude=src/mapreduce/5840-mrinput-*.txt" \
			"--exclude=src/main/mr-*" \
			"--exclude=mrtmp.*" \
			"--exclude=src/main/diff.out" \
			"--exclude=src/main/mrcoordinator" \
			"--exclude=src/main/mrsequential" \
			"--exclude=src/main/mrworker" \
			"--exclude=*.so" \
			Makefile src; \
		if test `stat -c "%s" "$@-handin.tar.gz" 2>/dev/null || stat -f "%z" "$@-handin.tar.gz"` -ge 20971520 ; then echo "File exceeds 20MB."; rm $@-handin.tar.gz; exit; fi; \
		echo "$@-handin.tar.gz successfully created. Please upload the tarball manually on Gradescope."; \
	else \
		echo "Bad target $@. Usage: make [$(LABS)]"; \
	fi

.PHONY: check-%
check-%:
	@echo "Checking that your submission builds correctly..."
	@./.check-build git://g.csail.mit.edu/6.5840-golabs-2023 $(patsubst check-%,%,$@)

# 并发测试目标 - 使用后台进程和临时文件（推荐）
.PHONY: test-many
test-many:
	@echo "开始并发测试: $(CMD)"
	@echo "并发数: $(W)"
	@echo "总次数: $(CNT)"
	@echo "=================================="
	@rm -f /tmp/test_results_*.log
	@for i in $$(seq 1 $(CNT)); do \
		sh -c "$(CMD)" > /tmp/test_results_$$i.log 2>&1 & \
		if [ $$((i % $(W))) -eq 0 ]; then \
			echo "等待批次 $$((i / $(W))) 完成..."; \
			wait; \
		fi; \
	done; \
	wait; \
	echo "所有测试完成，统计结果..."; \
	FAIL_COUNT=$$(grep -l "FAIL" /tmp/test_results_*.log 2>/dev/null | wc -l); \
	TOTAL_COUNT=$$(ls /tmp/test_results_*.log 2>/dev/null | wc -l); \
	echo "=================================="; \
	echo "总测试次数: $$TOTAL_COUNT"; \
	echo "失败次数: $$FAIL_COUNT"; \
	echo "成功率: $$(( (TOTAL_COUNT - FAIL_COUNT) * 100 / TOTAL_COUNT ))%"; \
	echo "失败率: $$(( FAIL_COUNT * 100 / TOTAL_COUNT ))%"; \
	if [ $$FAIL_COUNT -gt 0 ]; then \
		echo "失败日志文件:"; \
		grep -l "FAIL" /tmp/test_results_*.log 2>/dev/null | head -5; \
		if [ $$(grep -l "FAIL" /tmp/test_results_*.log 2>/dev/null | wc -l) -gt 5 ]; then \
			echo "... 还有更多失败文件"; \
		fi; \
		echo "失败日志内容示例:"; \
		grep -l "FAIL" /tmp/test_results_*.log 2>/dev/null | head -1 | xargs cat 2>/dev/null || true; \
	fi; \
#	echo "清理日志文件..."; \
#	rm -f /tmp/test_results_*.log

# 并发测试目标 - 使用tmux（需要安装tmux）
.PHONY: test-many-tmux
test-many-tmux:
	@echo "开始tmux并发测试: $(CMD)"
	@echo "并发数: $(W)"
	@echo "总次数: $(CNT)"
	@echo "=================================="
	@rm -f /tmp/tmux_test_results_*.log
	@for i in $$(seq 1 $(CNT)); do \
		tmux new-session -d -s test_$$i "$(CMD) > /tmp/tmux_test_results_$$i.log 2>&1; exit"; \
		if [ $$((i % $(W))) -eq 0 ]; then \
			echo "等待批次 $$((i / $(W))) 完成..."; \
			for j in $$(seq $$((i - $(W) + 1)) $$i); do \
				tmux wait-for -S test_$$j 2>/dev/null || true; \
			done; \
		fi; \
	done; \
	echo "等待所有测试完成..."; \
	for i in $$(seq 1 $(CNT)); do \
		tmux wait-for -S test_$$i 2>/dev/null || true; \
	done; \
	echo "所有测试完成，统计结果..."; \
	FAIL_COUNT=$$(grep -l "FAIL" /tmp/tmux_test_results_*.log 2>/dev/null | wc -l); \
	TOTAL_COUNT=$$(ls /tmp/tmux_test_results_*.log 2>/dev/null | wc -l); \
	echo "=================================="; \
	echo "总测试次数: $$TOTAL_COUNT"; \
	echo "失败次数: $$FAIL_COUNT"; \
	echo "成功率: $$(( (TOTAL_COUNT - FAIL_COUNT) * 100 / TOTAL_COUNT ))%"; \
	echo "失败率: $$(( FAIL_COUNT * 100 / TOTAL_COUNT ))%"; \
	if [ $$FAIL_COUNT -gt 0 ]; then \
		echo "失败日志文件:"; \
		grep -l "FAIL" /tmp/tmux_test_results_*.log 2>/dev/null | head -5; \
		if [ $$(grep -l "FAIL" /tmp/tmux_test_results_*.log 2>/dev/null | wc -l) -gt 5 ]; then \
			echo "... 还有更多失败文件"; \
		fi; \
	fi; \
	rm -f /tmp/tmux_test_results_*.log

# 并发测试目标 - 使用GNU parallel（需要安装parallel）
.PHONY: test-many-parallel
test-many-parallel:
	@echo "开始parallel并发测试: $(CMD)"
	@echo "并发数: $(W)"
	@echo "总次数: $(CNT)"
	@echo "=================================="
	@rm -f /tmp/parallel_test_results_*.log
	@seq 1 $(CNT) | parallel -j $(W) "$(CMD) > /tmp/parallel_test_results_{}.log 2>&1"
	@echo "所有测试完成，统计结果..."; \
	FAIL_COUNT=$$(grep -l "FAIL" /tmp/parallel_test_results_*.log 2>/dev/null | wc -l); \
	TOTAL_COUNT=$$(ls /tmp/parallel_test_results_*.log 2>/dev/null | wc -l); \
	echo "=================================="; \
	echo "总测试次数: $$TOTAL_COUNT"; \
	echo "失败次数: $$FAIL_COUNT"; \
	echo "成功率: $$(( (TOTAL_COUNT - FAIL_COUNT) * 100 / TOTAL_COUNT ))%"; \
	echo "失败率: $$(( FAIL_COUNT * 100 / TOTAL_COUNT ))%"; \
	if [ $$FAIL_COUNT -gt 0 ]; then \
		echo "失败日志文件:"; \
		grep -l "FAIL" /tmp/parallel_test_results_*.log 2>/dev/null | head -5; \
		if [ $$(grep -l "FAIL" /tmp/parallel_test_results_*.log 2>/dev/null | wc -l) -gt 5 ]; then \
			echo "... 还有更多失败文件"; \
		fi; \
	fi; \
	rm -f /tmp/parallel_test_results_*.log

# 清理临时文件
.PHONY: clean-test-logs
clean-test-logs:
	@echo "清理测试日志文件..."
	@rm -f /tmp/test_results_*.log
	@rm -f /tmp/tmux_test_results_*.log
	@rm -f /tmp/parallel_test_results_*.log
	@echo "清理完成" 