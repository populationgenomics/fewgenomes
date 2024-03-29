VERSION := v2
TEST_VERSION := v2
SCATTER_COUNT_TEST := 50
SCATTER_COUNT_PROD := 100
CALLSET := fewgenomes
REUSE_ARG := --reuse

.PHONY: joint_calling_update_submodule
joint_calling_update_submodule:
	-(cd ../joint-calling && git add --all && git commit -m 'WIP' --no-verify && git push)
	(cd joint-calling && git pull --rebase)
	(git add joint-calling && git commit -m 'Update joint-calling submodule' && git push)

.PHONY: joint_calling_test_to_tmp
joint_calling_test_to_tmp:
	analysis-runner \
	--dataset $(CALLSET) \
	--output-dir "joint-calling/test-to-tmp" \
	--description "Joint calling test-to-temporary" \
	--access-level test \
	joint-calling/driver_for_analysis_runner.sh workflows/batch_workflow.py \
	--scatter-count $(SCATTER_COUNT_TEST) \
	--from test \
	--to tmp \
	--batch batch1 \
	--callset $(CALLSET) \
	--version ${TEST_VERSION} \
	--keep-scratch \
	--skip-input-meta \
	$(REUSE_ARG)

.PHONY: joint_calling_test_to_test
joint_calling_test_to_test:
	analysis-runner \
	--dataset $(CALLSET) \
	--output-dir "joint-calling/test-to-test" \
	--description "Joint calling test-to-test" \
	--access-level test \
	joint-calling/driver_for_analysis_runner.sh workflows/batch_workflow.py \
	--scatter-count $(SCATTER_COUNT_TEST) \
	--from test \
	--to test \
	--batch batch1 \
	--callset $(CALLSET) \
	--version $(VERSION) \
	--keep-scratch \
	--skip-input-meta \
	$(REUSE_ARG)

.PHONY: joint_calling_main_to_main
joint_calling_main_to_main:
	analysis-runner \
	--dataset $(CALLSET) \
	--output-dir "joint-calling/main-to-main" \
	--description "Joint calling main-to-main" \
	--access-level test \
	joint-calling/driver_for_analysis_runner.sh workflows/batch_workflow.py \
	--scatter-count $(SCATTER_COUNT_PROD) \
	--batch batch1 \
	--from main \
	--to main \
	--callset $(CALLSET) \
	--version $(VERSION) \
	--skip-input-meta \
	$(REUSE_ARG)
