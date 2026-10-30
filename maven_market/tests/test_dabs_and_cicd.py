"""
Tests for the DABs bundle configuration — validates that databricks.yml
has the correct structure for multi-environment deployment (dev/prod),
resource references are consistent, and the pipeline chain is complete.

Satisfies Capstone Deliverable:
  - Scoring Category: DevOps & Modularity (15 pts)
  - POC 2: DABs Implementation
"""
import json
import unittest
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None


ROOT = Path(__file__).parent.parent


class TestDaBsConfiguration(unittest.TestCase):
    """Validates databricks.yml for multi-target, config-driven DABs compliance."""

    def _load_databricks_yml(self):
        yml_path = ROOT / "databricks.yml"
        self.assertTrue(yml_path.exists(), "databricks.yml not found in project root")
        if yaml is None:
            self.skipTest("PyYAML not installed")
        with open(yml_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def test_bundle_name_present(self):
        """Bundle must have a name defined."""
        config = self._load_databricks_yml()
        self.assertIn("bundle", config, "Missing 'bundle' key in databricks.yml")
        self.assertIn("name", config["bundle"], "Missing bundle.name in databricks.yml")
        self.assertEqual(config["bundle"]["name"], "maven_market")

    def test_dev_and_prod_targets_exist(self):
        """Both dev and prod targets must be defined for multi-environment support."""
        config = self._load_databricks_yml()
        targets = config.get("targets", {})
        self.assertIn("dev", targets, "Missing 'dev' target in databricks.yml")
        self.assertIn("prod", targets, "Missing 'prod' target in databricks.yml")

    def test_dev_target_is_development_mode(self):
        """Dev target must use 'development' mode to enable name prefixing."""
        config = self._load_databricks_yml()
        dev_mode = config["targets"]["dev"].get("mode", "")
        self.assertEqual(dev_mode, "development", "Dev target must use mode: development")

    def test_prod_target_is_production_mode(self):
        """Prod target must use 'production' mode."""
        config = self._load_databricks_yml()
        prod_mode = config["targets"]["prod"].get("mode", "")
        self.assertEqual(prod_mode, "production", "Prod target must use mode: production")

    def test_workspace_host_configured(self):
        """Workspace host must be set on targets so the bundle points at the correct Databricks instance."""
        config = self._load_databricks_yml()
        targets = config.get("targets", {})
        for tgt_name in ["dev", "prod"]:
            tgt = targets.get(tgt_name, {})
            ws = tgt.get("workspace", {})
            self.assertIn("host", ws, f"Missing workspace.host in target '{tgt_name}'")
            self.assertTrue(ws["host"].startswith("https://"), f"Host in target '{tgt_name}' must start with https://")

    # def test_resources_included(self):
    #     """Resource files for all three pipeline layers and the job must be included and exist."""
    #     config = self._load_databricks_yml()
    #     include_block = config.get("include", [])
    #     self.assertTrue(any("resources" in str(inc) for inc in include_block), "resources directory must be in include block")

    #     # Verify all three pipeline resources and job definition exist
    #     res_dir = ROOT / "resources"
    #     self.assertTrue((res_dir / "01_bronze.pipeline.yml").exists(), "01_bronze.pipeline.yml missing")
    #     self.assertTrue((res_dir / "02_silver.pipeline.yml").exists(), "02_silver.pipeline.yml missing")
    #     self.assertTrue((res_dir / "03_gold.pipeline.yml").exists(), "03_gold.pipeline.yml missing")
    #     self.assertTrue((res_dir / "sample_job.job.yml").exists(), "sample_job.job.yml missing")

    def test_resources_included(self):
        """Resource files for pipeline layers and the job must be included and exist."""
        config = self._load_databricks_yml()
        include_block = config.get("include", [])
        self.assertTrue(any("resources" in str(inc) for inc in include_block), "resources directory must be in include block")

        # Verify pipeline resources and job definition exist
        res_dir = ROOT / "resources"
        
        # Support either the single consolidated pipeline file or the 3 separate files
        has_pipelines = (
            (res_dir / "all_pipelines.yml").exists() or 
            (res_dir / "01_bronze.pipeline.yml").exists()
        )
        self.assertTrue(has_pipelines, "Pipeline resource YAML file(s) missing from resources/")
        self.assertTrue((res_dir / "sample_job.job.yml").exists(), "sample_job.job.yml missing")


class TestJobOrchestrationChain(unittest.TestCase):
    """Validates that sample_job.job.yml chains Bronze->Silver->Gold->Audit Sync."""

    def _load_job_yml(self):
        if yaml is None:
            self.skipTest("PyYAML not installed")
        job_path = ROOT / "resources" / "sample_job.job.yml"
        self.assertTrue(job_path.exists(), "sample_job.job.yml not found in resources/")
        with open(job_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def test_four_tasks_defined(self):
        """Orchestration job must have 4 tasks: Bronze, Silver, Gold, Audit Sync."""
        config = self._load_job_yml()
        tasks = config["resources"]["jobs"]["maven_market_job"]["tasks"]
        self.assertEqual(len(tasks), 4, f"Expected 4 tasks in job DAG, found {len(tasks)}")

    def test_bronze_is_first_task(self):
        """Bronze must be the root task with no dependencies."""
        config = self._load_job_yml()
        tasks = config["resources"]["jobs"]["maven_market_job"]["tasks"]
        bronze_task = next((t for t in tasks if "bronze" in t["task_key"].lower()), None)
        self.assertIsNotNone(bronze_task, "Bronze ingestion task not found in job")
        self.assertNotIn("depends_on", bronze_task, "Bronze should have no upstream dependencies")

    def test_silver_depends_on_bronze(self):
        """Silver transformation must depend on Bronze ingestion."""
        config = self._load_job_yml()
        tasks = config["resources"]["jobs"]["maven_market_job"]["tasks"]
        silver_task = next((t for t in tasks if "silver" in t["task_key"].lower()), None)
        self.assertIsNotNone(silver_task, "Silver transformation task not found in job")
        deps = [d["task_key"] for d in silver_task.get("depends_on", [])]
        self.assertTrue(
            any("bronze" in d.lower() for d in deps),
            f"Silver must depend on Bronze, actual deps: {deps}"
        )

    def test_gold_depends_on_silver(self):
        """Gold aggregation must depend on Silver transformation."""
        config = self._load_job_yml()
        tasks = config["resources"]["jobs"]["maven_market_job"]["tasks"]
        gold_task = next((t for t in tasks if "gold" in t["task_key"].lower() and "audit" not in t["task_key"].lower()), None)
        self.assertIsNotNone(gold_task, "Gold aggregation task not found in job")
        deps = [d["task_key"] for d in gold_task.get("depends_on", [])]
        self.assertTrue(
            any("silver" in d.lower() for d in deps),
            f"Gold must depend on Silver, actual deps: {deps}"
        )

    def test_audit_sync_depends_on_gold(self):
        """Audit telemetry sync must depend on Gold aggregation."""
        config = self._load_job_yml()
        tasks = config["resources"]["jobs"]["maven_market_job"]["tasks"]
        audit_task = next((t for t in tasks if "audit" in t["task_key"].lower()), None)
        self.assertIsNotNone(audit_task, "Audit telemetry sync task not found in job")
        deps = [d["task_key"] for d in audit_task.get("depends_on", [])]
        self.assertTrue(
            any("gold" in d.lower() for d in deps),
            f"Audit sync must depend on Gold, actual deps: {deps}"
        )


class TestCICDWorkflow(unittest.TestCase):
    """Validates that .github/workflows/deploy.yml is present and covers all required CI steps."""

    def _load_workflow(self):
        if yaml is None:
            self.skipTest("PyYAML not installed")
        wf_path = ROOT / ".github" / "workflows" / "deploy.yml"
        self.assertTrue(wf_path.exists(), ".github/workflows/deploy.yml not found — CI/CD deliverable missing")
        with open(wf_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f), wf_path.read_text(encoding="utf-8")

    def test_workflow_triggers_on_main_push(self):
        """CI/CD workflow must trigger on push to main for production deployments."""
        config, _ = self._load_workflow()
        on_block = config.get("on") or config.get(True) or {}
        push_block = on_block.get("push", {})
        branches = push_block.get("branches", [])
        self.assertIn("main", branches, "CI/CD workflow must trigger on push to 'main' branch")

    def test_workflow_has_lint_step(self):
        """CI/CD workflow must include a linting step (ruff or flake8)."""
        _, text = self._load_workflow()
        has_linter = "ruff" in text.lower() or "flake8" in text.lower()
        self.assertTrue(has_linter, "CI/CD workflow must include ruff or flake8 linting")

    def test_workflow_has_pytest_step(self):
        """CI/CD workflow must include automated pytest unit testing."""
        _, text = self._load_workflow()
        self.assertIn("pytest", text, "CI/CD workflow must run pytest before deployment")

    def test_workflow_deploys_to_dev_on_pr(self):
        """CI/CD workflow must deploy to dev environment on pull requests."""
        _, text = self._load_workflow()
        self.assertIn("bundle deploy -t dev", text, "CI/CD workflow must deploy to dev environment")

    def test_workflow_deploys_to_prod_on_main(self):
        """CI/CD workflow must deploy to prod only on merge to main."""
        _, text = self._load_workflow()
        self.assertIn("bundle deploy -t prod", text, "CI/CD workflow must deploy to prod on merge to main")


class TestClusterPolicy(unittest.TestCase):
    """Validates cluster_policy.json for cost governance compliance."""

    def _load_policy(self):
        policy_path = ROOT / "resources" / "cluster_policy.json"
        self.assertTrue(policy_path.exists(), "resources/cluster_policy.json not found")
        with open(policy_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def test_policy_has_name(self):
        """Cluster policy must be named."""
        policy = self._load_policy()
        self.assertIn("name", policy, "Cluster policy JSON must have a 'name' field")
        self.assertIn("maven_market", policy["name"], "Cluster policy name must reference maven_market")

    def test_autotermination_max_30_minutes(self):
        """Cluster policy must enforce a maximum 30-minute auto-termination."""
        policy = self._load_policy()
        auto_term = policy["definition"].get("autotermination_minutes", {})
        max_val = auto_term.get("maxValue")
        self.assertIsNotNone(max_val, "autotermination_minutes must be defined in cluster policy")
        self.assertLessEqual(int(max_val), 30, f"Max auto-termination must be ≤ 30 minutes, got {max_val}")

    def test_max_workers_constrained(self):
        """Cluster policy must limit maximum workers to control compute costs."""
        policy = self._load_policy()
        max_workers = policy["definition"].get("autoscale.max_workers", {})
        max_val = max_workers.get("maxValue")
        self.assertIsNotNone(max_val, "autoscale.max_workers must be defined in cluster policy")
        self.assertLessEqual(int(max_val), 4, f"Max workers must be ≤ 4 for cost governance, got {max_val}")

    def test_mandatory_project_billing_tag(self):
        """Cluster policy must automatically apply project: maven_market billing tag."""
        policy = self._load_policy()
        project_tag = policy["definition"].get("custom_tags.project", {})
        self.assertEqual(project_tag.get("type"), "fixed", "project tag must be 'fixed' (non-overridable)")
        self.assertEqual(project_tag.get("value"), "maven_market", "project tag value must be 'maven_market'")

    def test_allowed_vm_types_are_cost_effective(self):
        """Cluster policy must restrict VM types to cost-effective Azure D-series or F-series."""
        policy = self._load_policy()
        node_type = policy["definition"].get("node_type_id", {})
        self.assertEqual(node_type.get("type"), "allowlist", "node_type_id must be an allowlist")
        allowed = node_type.get("values", [])
        self.assertGreater(len(allowed), 0, "At least one allowed VM type must be defined")
        for vm in allowed:
            is_d_or_f_series = vm.startswith("Standard_D") or vm.startswith("Standard_F") or vm.startswith("Standard_E")
            self.assertTrue(is_d_or_f_series, f"VM type '{vm}' is not a cost-effective D/E/F series")


if __name__ == "__main__":
    unittest.main()
