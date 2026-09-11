"""Layout tests for atlas-kafka (KRaft scheme B)."""

from __future__ import annotations

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class KafkaLayoutTest(unittest.TestCase):
    def test_playbook_exists(self) -> None:
        self.assertTrue((REPO_ROOT / "playbooks" / "kafka_cluster.yaml").is_file())

    def test_roles_exist(self) -> None:
        for role in (
            "00_ensure_workspace",
            "01_validate_vars",
            "200_kafka_node",
            "201_kafka_controllers",
            "202_kafka_brokers",
            "203_kafka_verify",
        ):
            self.assertTrue((REPO_ROOT / "roles" / role / "tasks" / "main.yaml").is_file(), role)

    def test_workspace_role_and_catalog(self) -> None:
        catalog = (REPO_ROOT / "group_vars" / "all" / "atlas-kafka.yml").read_text(encoding="utf-8")
        self.assertIn("REQUIRED", catalog)
        self.assertIn("OPTIONAL", catalog)
        self.assertIn("cluster_workspace_root:", catalog)
        self.assertIn("cluster_workspace_parent:", catalog)
        self.assertIn('cluster_domain: kafka.example.com', catalog)
        self.assertTrue((REPO_ROOT / "roles" / "00_ensure_workspace" / "defaults" / "main.yml").is_file())
        playbook = (REPO_ROOT / "playbooks" / "kafka_cluster.yaml").read_text(encoding="utf-8")
        self.assertIn("00_ensure_workspace", playbook)
        self.assertIn("tags: [00_ensure_workspace]", playbook)
        self.assertLess(
            playbook.index("00_ensure_workspace"),
            playbook.index("01_validate_vars"),
        )
        node_defaults = (REPO_ROOT / "roles" / "200_kafka_node" / "defaults" / "main.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("cluster_workspace_root", node_defaults)
        self.assertIn("CLUSTER_WORKSPACE_ROOT", node_defaults)
        self.assertIn(".cache/atlas-kafka", node_defaults)
        create_dirs = (
            REPO_ROOT / "roles" / "00_ensure_workspace" / "tasks" / "create_dirs.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn(".cache/atlas-kafka", create_dirs)
        for role in ("201_kafka_controllers", "202_kafka_brokers"):
            defaults = (REPO_ROOT / "roles" / role / "defaults" / "main.yml").read_text(
                encoding="utf-8"
            )
            self.assertIn("cluster_workspace_root", defaults, role)
            self.assertIn("CLUSTER_WORKSPACE_ROOT", defaults, role)

    def test_no_legacy_zookeeper_or_tf_dirs(self) -> None:
        self.assertFalse((REPO_ROOT / "400_kafka_zookeeper").exists())
        self.assertFalse((REPO_ROOT / "401_kafka_broker").exists())
        self.assertFalse((REPO_ROOT / "402_kafka_lb").exists())
        self.assertFalse((REPO_ROOT / "00_tf_kafka_nodes_ansible").exists())

    def test_playbook_order_controllers_before_brokers(self) -> None:
        text = (REPO_ROOT / "playbooks" / "kafka_cluster.yaml").read_text(encoding="utf-8")
        self.assertIn("kafka_controller_hosts", text)
        self.assertIn("kafka_broker_hosts", text)
        self.assertIn("kafka_controllers", text)  # default host pattern
        self.assertIn("kafka_brokers", text)
        self.assertLess(text.index("201_kafka_controllers"), text.index("202_kafka_brokers"))
        self.assertNotIn("zookeeper", text.lower())
        self.assertNotIn("kafka_lbs", text)

    def test_validate_rejects_zookeeper_and_requires_odd_controllers(self) -> None:
        text = (REPO_ROOT / "roles" / "01_validate_vars" / "tasks" / "main.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("groups[kafka_controller_hosts]", text)
        self.assertIn("groups[kafka_broker_hosts]", text)
        self.assertIn("% 2) == 1", text)
        self.assertIn("zookeeper", text)
        self.assertIn("kafka_lbs", text)
        defaults = (REPO_ROOT / "roles" / "01_validate_vars" / "defaults" / "main.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("kafka_controller_hosts: kafka_controllers", defaults)
        self.assertIn("kafka_broker_hosts: kafka_brokers", defaults)

    def test_defaults_pin_kafka_431(self) -> None:
        defaults = (REPO_ROOT / "roles" / "200_kafka_node" / "defaults" / "main.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn('kafka_version: "4.3.1"', defaults)
        self.assertIn('kafka_scala_version: "2.13"', defaults)
        self.assertIn("kafka_download_default_url", defaults)
        self.assertIn("stage_kafka_archive.yaml", 
            (REPO_ROOT / "roles" / "200_kafka_node" / "tasks" / "main.yaml").read_text(encoding="utf-8"))

    def test_controller_and_broker_configs_are_kraft(self) -> None:
        ctrl = (
            REPO_ROOT / "roles" / "201_kafka_controllers" / "templates" / "controller.properties.j2"
        ).read_text(encoding="utf-8")
        broker = (
            REPO_ROOT / "roles" / "202_kafka_brokers" / "templates" / "broker.properties.j2"
        ).read_text(encoding="utf-8")
        self.assertIn("process.roles=controller", ctrl)
        self.assertIn("controller.quorum.voters=", ctrl)
        self.assertNotIn("zookeeper.connect", ctrl)
        self.assertIn("process.roles=broker", broker)
        self.assertIn("controller.quorum.voters=", broker)
        self.assertIn("advertised.listeners=", broker)
        self.assertNotIn("zookeeper.connect", broker)
        self.assertIn("advertised.listeners=", ctrl)

    def test_validate_requires_stable_kafka_node_id(self) -> None:
        text = (REPO_ROOT / "roles" / "01_validate_vars" / "tasks" / "main.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("kafka_node_id", text)
        self.assertIn("unique", text)

    def test_roles_notify_restart_on_config_change(self) -> None:
        for role in ("201_kafka_controllers", "202_kafka_brokers"):
            text = (REPO_ROOT / "roles" / role / "tasks" / "main.yaml").read_text(encoding="utf-8")
            self.assertIn("notify: Restart Kafka", text, role)

    def test_binary_upgrade_restarts_only_when_unit_exists(self) -> None:
        text = (REPO_ROOT / "roles" / "200_kafka_node" / "tasks" / "main.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("Restart Kafka after binary upgrade when unit already exists", text)
        self.assertNotIn("notify: Restart Kafka", text)
        self.assertFalse((REPO_ROOT / "roles" / "200_kafka_node" / "handlers" / "main.yaml").exists())

    def test_node_install_supports_ubuntu_debian_oracle(self) -> None:
        defaults = (REPO_ROOT / "roles" / "200_kafka_node" / "defaults" / "main.yml").read_text(
            encoding="utf-8"
        )
        tasks = (REPO_ROOT / "roles" / "200_kafka_node" / "tasks" / "main.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("kafka_java_package_ubuntu", defaults)
        self.assertIn("kafka_java_package_debian", defaults)
        self.assertIn("kafka_java_package_oracle", defaults)
        self.assertIn("kafka_user_shell", defaults)
        self.assertIn("openjdk-17-jre-headless", defaults)
        self.assertIn("openjdk-21-jre-headless", defaults)
        self.assertIn("Install Java runtime (Ubuntu)", tasks)
        self.assertIn("Install Java runtime (Debian)", tasks)
        self.assertIn("Install Java runtime (Oracle Linux)", tasks)
        self.assertIn("ansible.builtin.dnf", tasks)
        self.assertIn("OracleLinux", tasks)
        self.assertIn("distribution_version", tasks)
        self.assertIn("kafka_user_shell", tasks)
        self.assertIn("kafka_firewalld_enabled", defaults)
        self.assertIn("Open Kafka ports in firewalld (Oracle Linux)", tasks)
        self.assertIn("ansible.posix.firewalld", tasks)
        self.assertIn("kafka_firewalld_ports", tasks)
        self.assertIn("kafka_controllers", tasks)
        self.assertIn("kafka_brokers", tasks)

    def test_kraft_meta_validation_task_exists(self) -> None:
        text = (
            REPO_ROOT / "roles" / "200_kafka_node" / "tasks" / "validate_kraft_meta.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("meta.properties cluster.id", text)
        self.assertIn("meta.properties node.id", text)
        for role in ("201_kafka_controllers", "202_kafka_brokers"):
            role_text = (REPO_ROOT / "roles" / role / "tasks" / "main.yaml").read_text(encoding="utf-8")
            if role == "201_kafka_controllers":
                role_text = (
                    REPO_ROOT / "roles" / role / "tasks" / "format_storage.yaml"
                ).read_text(encoding="utf-8")
            self.assertIn("validate_kraft_meta.yaml", role_text, role)

    def test_broker_waits_for_port_and_registration(self) -> None:
        text = (REPO_ROOT / "roles" / "202_kafka_brokers" / "tasks" / "main.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("flush_handlers", text)
        self.assertIn("wait_for:", text)
        self.assertIn("kafka-broker-api-versions.sh", text)
        self.assertIn("kafka_broker_registration_probe", text)

    def test_verify_smoke_uses_bootstrap_brokers(self) -> None:
        verify = (REPO_ROOT / "roles" / "203_kafka_verify" / "tasks" / "main.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("kafka-broker-api-versions.sh", verify)
        self.assertIn("kafka-console-producer.sh", verify)
        self.assertIn("kafka-console-consumer.sh", verify)
        self.assertIn("kafka_verify_bootstrap", verify)
        self.assertIn("regex_findall", verify)
        self.assertIn("kafka_verify_reported_broker_ids", verify)
        self.assertNotIn("kafka_verify_consumer_group", verify)
        self.assertIn("--from-beginning", verify)
        defaults = (REPO_ROOT / "roles" / "203_kafka_verify" / "defaults" / "main.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("atlas_kafka_verify", defaults)
        self.assertNotIn("__atlas_kafka_verify", defaults)
        ctrl_main = (
            REPO_ROOT / "roles" / "201_kafka_controllers" / "tasks" / "main.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("kafka-metadata-quorum.sh", ctrl_main)

    def test_standalone_entrypoints(self) -> None:
        run_sh = REPO_ROOT / "run.sh"
        self.assertTrue(run_sh.is_file())
        self.assertTrue(run_sh.stat().st_mode & 0o111, "run.sh must be executable")
        self.assertTrue((REPO_ROOT / "inventory-example.yml").is_file())
        self.assertTrue((REPO_ROOT / "group_vars" / "all" / "atlas-kafka.yml").is_file())
        self.assertTrue((REPO_ROOT / "group_vars" / "all" / "atlas-kafka.secrets.yml").is_file())
        self.assertTrue((REPO_ROOT / "requirements.yml").is_file())
        self.assertTrue((REPO_ROOT / "examples" / "secrets.example.yml").is_file())
        self.assertTrue((REPO_ROOT / "examples" / "hosts.example.yml").is_file())
        self.assertTrue((REPO_ROOT / "host_vars" / "example.yml").is_file())
        catalog = (REPO_ROOT / "group_vars" / "all" / "atlas-kafka.yml").read_text(encoding="utf-8")
        self.assertIn("REQUIRED", catalog)
        self.assertIn("OPTIONAL", catalog)
        self.assertIn('kafka_version: "4.3.1"', catalog)
        self.assertIn("cluster_workspace_root:", catalog)
        self.assertIn("kafka_security_mode: plaintext", catalog)
        self.assertIn("kafka_controller_hosts:", catalog)
        self.assertIn("kafka_broker_hosts:", catalog)
        inventory = (REPO_ROOT / "inventory-example.yml").read_text(encoding="utf-8")
        self.assertIn("kafka_controllers", inventory)
        self.assertIn("kafka_brokers", inventory)
        self.assertIn("kafka_node_id:", inventory)
        playbook = (REPO_ROOT / "playbooks" / "kafka_cluster.yaml").read_text(encoding="utf-8")
        self.assertIn("kafka_controller_hosts", playbook)
        self.assertIn("kafka_broker_hosts", playbook)
        self.assertIn("kafka_controllers", playbook)  # default host pattern
        self.assertIn("kafka_brokers", playbook)
        validate = (REPO_ROOT / "roles" / "01_validate_vars" / "tasks" / "main.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("groups[kafka_controller_hosts]", validate)
        self.assertIn("groups[kafka_broker_hosts]", validate)
        self.assertNotIn("group_vars/all/kafka.yml", validate)
        run_text = run_sh.read_text(encoding="utf-8")
        self.assertIn("playbooks/kafka_cluster.yaml", run_text)
        self.assertIn("CLUSTER_WORKSPACE_ROOT", run_text)
        self.assertIn("EXTRA_VARS_FILE", run_text)
        req = (REPO_ROOT / "requirements.yml").read_text(encoding="utf-8")
        self.assertIn("collections:", req)

    def test_readme_is_standalone_first(self) -> None:
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("./run.sh", readme)
        self.assertIn("Quickstart", readme)
        self.assertIn("group_vars/all/atlas-kafka.yml", readme)
        self.assertIn("inventory-example.yml", readme)
        self.assertIn("Troubleshooting", readme)
        self.assertIn("Greenfield checklist", readme)
        self.assertIn("Contributing", readme)
        self.assertIn("Project structure", readme)
        self.assertIn("00_ensure_workspace", readme)
        # Primary operator path must not be an external orchestrator CLI
        self.assertNotIn("./cluster ", readme)
        self.assertNotIn("./cluster\n", readme)
        self.assertNotIn("clusterctl", readme)
        quick = readme.index("## Quickstart")
        integ = readme.index("## Integrations")
        self.assertLess(quick, integ)
        # Product docs must not prescribe org lab hostnames
        self.assertNotIn("mxhash", readme.lower())

    def test_no_org_fingerprint_in_product_sources(self) -> None:
        needles = ("mxhash", "gitea.", "harbor.", "welcomeback", "upload.mxhash")
        hits = _scan_product_sources_for(needles)
        self.assertEqual(hits, [], f"org fingerprint in product sources: {hits}")

        # Orchestrator split-file overlay path must not be the product SoT.
        overlay_hits = _scan_product_sources_for(("group_vars/all/kafka.yml",), case_sensitive=True)
        self.assertEqual(overlay_hits, [], f"orchestrator overlay path in product sources: {overlay_hits}")

        # Comments must stay vendor-neutral (no org package registry product name).
        for rel in (
            "roles/200_kafka_node/defaults/main.yml",
            "roles/201_kafka_controllers/defaults/main.yml",
            "roles/202_kafka_brokers/defaults/main.yml",
        ):
            text = (REPO_ROOT / rel).read_text(encoding="utf-8")
            self.assertNotIn("Nexus", text, rel)
            self.assertNotIn("group_vars/all/kafka.yml", text, rel)


_PRODUCT_SCAN_ROOTS = (
    REPO_ROOT / "roles",
    REPO_ROOT / "playbooks",
    REPO_ROOT / "group_vars",
    REPO_ROOT / "examples",
    REPO_ROOT / "host_vars",
    REPO_ROOT / "inventory-example.yml",
    REPO_ROOT / "run.sh",
    REPO_ROOT / "ansible.cfg",
    REPO_ROOT / "requirements.yml",
    REPO_ROOT / "README.md",
)
_PRODUCT_SCAN_SUFFIXES = {".yml", ".yaml", ".j2", ".sh", ".cfg", ".md", ".py"}


def _scan_product_sources_for(
    needles: tuple[str, ...],
    *,
    case_sensitive: bool = False,
) -> list[str]:
    hits: list[str] = []
    for root in _PRODUCT_SCAN_ROOTS:
        paths = [root] if root.is_file() else root.rglob("*")
        for path in paths:
            if not path.is_file():
                continue
            if path.suffix.lower() not in _PRODUCT_SCAN_SUFFIXES and path.name != "run.sh":
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            haystack = text if case_sensitive else text.lower()
            for needle in needles:
                probe = needle if case_sensitive else needle.lower()
                if probe in haystack:
                    hits.append(f"{path.relative_to(REPO_ROOT)}:{needle}")
    return hits


class PublishHygieneTest(unittest.TestCase):
    """Contracts that must hold before publishing the repo."""

    def test_license_and_security_files(self) -> None:
        self.assertTrue((REPO_ROOT / "LICENSE").is_file())
        self.assertTrue((REPO_ROOT / "SECURITY.md").is_file())
        self.assertTrue((REPO_ROOT / "README.md").is_file())
        self.assertTrue((REPO_ROOT / ".ansible-lint").is_file())
        self.assertTrue((REPO_ROOT / ".github" / "workflows" / "ci.yml").is_file())
        self.assertTrue((REPO_ROOT / "requirements-dev.txt").is_file())

    def test_ci_entrypoints_executable(self) -> None:
        run_sh = REPO_ROOT / "run.sh"
        run_ci = REPO_ROOT / "tests" / "run_ci.sh"
        self.assertTrue(run_sh.is_file())
        self.assertTrue(run_sh.stat().st_mode & 0o111, "run.sh must be executable")
        self.assertTrue(run_ci.is_file())
        self.assertTrue(run_ci.stat().st_mode & 0o111, "tests/run_ci.sh must be executable")

    def test_no_obvious_secret_material_in_product_sources(self) -> None:
        needles = ("Welcomeback", "BEGIN OPENSSH PRIVATE", "BEGIN RSA PRIVATE", "AKIA", "gldt-")
        hits = _scan_product_sources_for(needles, case_sensitive=True)
        self.assertEqual(hits, [], f"secret-like material found: {hits}")

    def test_readme_mentions_ci_runner(self) -> None:
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("./tests/run_ci.sh", readme)
        self.assertIn("Testing / CI", readme)
        self.assertIn(".github/workflows/ci.yml", readme)


if __name__ == "__main__":
    unittest.main()
