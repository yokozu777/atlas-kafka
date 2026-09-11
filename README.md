# atlas-kafka

Ansible playbooks for an Apache Kafka **KRaft** cluster (scheme B): dedicated
controllers + brokers. No ZooKeeper. No L4 VIP/HAProxy on the Kafka protocol path.

**Canonical playbook:** `playbooks/kafka_cluster.yaml`  
**Runner:** `./run.sh`  
**Vars catalog:** `group_vars/all/atlas-kafka.yml`  
**Secrets overlay:** `group_vars/all/atlas-kafka.secrets.yml` (Vault-friendly; empty for plaintext)  
**License:** Apache-2.0 (see `LICENSE`)

```
  Clients
     |
     v
  bootstrap.servers = broker1:9092,broker2:9092,broker3:9092
     |
     +-----> kafka_brokers       (data / advertised.listeners)
     |
     +-- metadata <-- kafka_controllers  (Raft quorum, port 9093)
```

Role pipeline:

```
  validate --> 200_kafka_node --> 201_controllers --> 202_brokers --> 203_verify
```

No ZooKeeper. No L4 VIP on the Kafka protocol path.  
Orchestrated path: **external orchestrator** (`templates → provision → init → kafka`).

## Compatibility

Targeted at:
- **Ubuntu** (OpenJDK 17 via apt: `openjdk-17-jre-headless`)
- **Debian** (OpenJDK 21 via apt: `openjdk-21-jre-headless` — Debian 13 has no OpenJDK 17 in main)
- **Oracle Linux** / RHEL-family (OpenJDK via dnf: `java-17-openjdk-headless`)

CI mixed-OS fleet SoT: `atlas-inventory/clusters/ci/MIXED_OS_KAFKA_REDIS.md`.

Inventory contract (required groups — override names via vars if needed):

| Group (default) | Targeting var | Roles |
|-----------------|---------------|-------|
| `kafka_controllers` | `kafka_controller_hosts` | `200_kafka_node`, `201_kafka_controllers` |
| `kafka_brokers` | `kafka_broker_hosts` | `200_kafka_node`, `202_kafka_brokers`, `203_kafka_verify` |

Optional parent group `kafka` may nest both groups for readability (see `inventory-example.yml`).

Sizing is enforced by `01_validate_vars`: controllers odd ≥ 3, brokers ≥ 3.

To use different inventory group names (single group each):

```yaml
# group_vars/all/atlas-kafka.yml
kafka_controller_hosts: my_kafka_controllers
kafka_broker_hosts: my_kafka_brokers
```

Every host must define a stable unique **`kafka_node_id`**:

| Role | Range (defaults) |
|------|------------------|
| controllers | `1` … `kafka_controller_node_id_max` (`99`) |
| brokers | ≥ `kafka_broker_node_id_base` (`100`) |

Do **not** change node IDs after KRaft storage is formatted. Controllers and brokers must not share hosts (combined nodes are rejected by validate). Legacy inventory groups `zookeeper` and `kafka_lbs` are rejected.

## Quickstart

### Prerequisites

- Ansible 2.14+ recommended
- `ansible-galaxy collection install -r requirements.yml` (`ansible.posix` for firewalld on Oracle Linux)
- SSH access to targets (Python 3 on targets)
- Outbound or mirrored access to download the Kafka binary tarball (or set `kafka_download_mirror_url` / `kafka_download_url`)
- Open TCP between nodes: controller port (`9093`) among controllers (and to brokers for Raft), broker port (`9092`) among brokers and from clients
- On **Oracle Linux**, role `200_kafka_node` opens `kafka_controller_port` / `kafka_broker_port` in firewalld when the service is running (`kafka_firewalld_enabled`, default true). Ubuntu/Debian baseline firewall remains the operator's responsibility if enabled.

### Clone

```bash
git clone <atlas-kafka-url>
cd atlas-kafka
ansible-galaxy collection install -r requirements.yml
```

### Inventory

```bash
cp inventory-example.yml inventory.yml
vi inventory.yml
```

Set `ansible_host`, **`hostname:`** (prefer FQDN), and **`kafka_node_id`** on every node. Advertised listeners default to `ansible_host` (brokers) / inventory addressing used in quorum voters.

### Variables

**SoT:** `group_vars/all/atlas-kafka.yml` — split into **REQUIRED** (inventory group targeting, workspace, connection, cluster identity/ports) and **OPTIONAL** tunables.  
Deep knobs live in `roles/*/defaults/main.yml` when not listed in `group_vars/all/atlas-kafka.yml`.

Lab default is `kafka_security_mode: plaintext` (no AUTH). Minimum identity to set for a real deploy:

```yaml
cluster_domain: kafka.example.com
ansible_user: localuser
kafka_version: "4.3.1"
kafka_scala_version: "2.13"
kafka_broker_port: 9092
kafka_controller_port: 9093
```

Optional overlay (mirror URL, future secrets — keep outside git):

```bash
cp examples/secrets.example.yml ~/kafka-overlay.yml
# edit, optionally: ansible-vault encrypt ~/kafka-overlay.yml
EXTRA_VARS_FILE=~/kafka-overlay.yml ./run.sh
```

### Run

```bash
./run.sh --tags 01_validate_vars   # contract check (localhost)
./run.sh                           # full stack
```

Recommended staged tags (same order as the playbook):

```bash
./run.sh --tags 200_kafka_node
./run.sh --tags 201_kafka_controllers
./run.sh --tags 202_kafka_brokers
./run.sh --tags 203_kafka_verify
```

Or manually:

```bash
ansible-playbook -i inventory.yml playbooks/kafka_cluster.yaml
```

### `./run.sh` environment

| Variable | Default | Meaning |
|----------|---------|---------|
| `INVENTORY` | `inventory.yml` or `inventory-example.yml` | Inventory path |
| `PLAYBOOK` | `playbooks/kafka_cluster.yaml` | Playbook path |
| `EXTRA_VARS_FILE` | — | Optional `-e @file` (vaulted overlay) |
| `SSH_KEY` / `ANSIBLE_PRIVATE_KEY_FILE` | `~/.ssh/id_ed25519` or `id_rsa` | SSH private key |
| `CLUSTER_WORKSPACE_ID` | `kafka.example.com` | Workspace dir name under `./workspace` |
| `CLUSTER_WORKSPACE_ROOT` | `./workspace/<id>` | Full controller workspace (tarball / `cluster.id` cache) |
| `ANSIBLE_CONFIG` | `./ansible.cfg` | Ansible config |

All extra CLI arguments are forwarded to `ansible-playbook`.

## Architecture (role order)

`playbooks/kafka_cluster.yaml`:

```
  ensure_workspace --> validate
         |
         v
  200_kafka_node --> 201_controllers --> 202_brokers --> 203_verify
```

1. **00_ensure_workspace** (localhost, tag `00_ensure_workspace`) — controller workspace + `.cache/atlas-kafka`  
2. **01_validate_vars** (localhost) — inventory sizing, unique `kafka_node_id`, no ZooKeeper / LB groups, no controller∩broker overlap  
3. **200_kafka_node** — Java + official binary tarball on controllers and brokers (archive staged on the Ansible controller)  
4. **201_kafka_controllers** — resolve/persist `cluster.id`, format KRaft storage, start controller quorum  
5. **202_kafka_brokers** — format broker storage, start brokers, wait for API + registration  
6. **203_kafka_verify** — `kafka-broker-api-versions` + produce/consume smoke on a temp topic

Clients typically connect:

| Path | Port (defaults) | Notes |
|------|-----------------|-------|
| Bootstrap brokers | `kafka_broker_port` → `9092` | Comma-separated broker list (not a VIP) |
| Controller Raft | `kafka_controller_port` → `9093` | Not for client produce/consume |

Lab uses a **static** KRaft quorum (`kraft.version=0`): both controllers and brokers set `controller.quorum.voters`. Cluster ID is generated once and stored under `kafka_data_dir` / workspace marker (`.cache/atlas-kafka/cluster.id`).

## Kafka install (200)

| Variable | Default | Purpose |
|----------|---------|---------|
| `kafka_version` | `4.3.1` | Apache Kafka release |
| `kafka_scala_version` | `2.13` | Scala binary line in the official tarball name |
| `kafka_heap_opts` | `-Xms1G -Xmx1G` | JVM heap for the Kafka process |
| `kafka_download_mirror_url` | `""` | Full URL override (HTTP(S) mirror) |
| `kafka_download_url` | `""` | Alternate full URL; empty → Apache downloads |
| `kafka_download_timeout` | `600` | Controller `get_url` timeout (seconds) |
| `kafka_security_mode` | `plaintext` | Lab v1: `plaintext` only |

Download priority: `kafka_download_mirror_url` → `kafka_download_url` → Apache `downloads.apache.org`.

Controller staging prefers `CLUSTER_WORKSPACE_ROOT` (from `./run.sh` or env), then `cluster_workspace_root` from group_vars, then `$HOME`. Role `00_ensure_workspace` creates `./workspace/<id>/.cache/atlas-kafka` on the controller.

## Controllers (201)

| Variable | Default | Purpose |
|----------|---------|---------|
| `kafka_controller_port` | `9093` | Raft / CONTROLLER listener |
| `kafka_controller_node_id_max` | `99` | Max allowed `kafka_node_id` for controllers |
| `kafka_controller_service_retries` | `30` | Wait loops after start |
| `kafka_controller_service_delay` | `2` | Seconds between retries |

Format runs once per node (storage under `kafka_data_dir`). Changing `kafka_node_id` or wiping only some nodes without a planned rebuild will fail `meta.properties` validation.

## Brokers (202)

| Variable | Default | Purpose |
|----------|---------|---------|
| `kafka_broker_port` | `9092` | Client / inter-broker PLAINTEXT listener |
| `kafka_broker_node_id_base` | `100` | Min allowed `kafka_node_id` for brokers |
| `kafka_broker_service_retries` | `30` | Wait loops after start |
| `kafka_broker_service_delay` | `2` | Seconds between retries |

Brokers advertise `ansible_host` (override via role defaults / host vars if needed). Replication factors for internal topics scale with broker count (capped at 3).

## Verify (203)

| Variable | Default | Purpose |
|----------|---------|---------|
| `kafka_verify_topic` | `atlas_kafka_verify` | Ephemeral smoke topic (deleted/recreated each verify) |
| `kafka_verify_timeout_ms` | `30000` | Producer/consumer timeouts |
| `kafka_verify_retries` | `20` | API-versions / consume retries |
| `kafka_verify_delay` | `3` | Seconds between retries |

Verify asserts the number of distinct broker IDs reported by `kafka-broker-api-versions` matches inventory, then produce/consume a unique message.

## Greenfield checklist

| Check | Expectation |
|-------|-------------|
| Controllers | `systemctl is-active kafka` → `active` on all `kafka_controllers` |
| Quorum | `kafka-metadata-quorum.sh --bootstrap-controller <c1>:9093 describe --status` shows a live quorum |
| Brokers | `systemctl is-active kafka` → `active` on all `kafka_brokers` |
| API versions | `kafka-broker-api-versions.sh --bootstrap-server <b1>:9092,…` lists every broker id |
| Smoke | `./run.sh --tags 203_kafka_verify` succeeds (produce + consume) |
| Clients | App `bootstrap.servers` = broker list on `9092` (not controllers, not a round-robin VIP) |

## Troubleshooting

### Validate fails: odd controllers / sizing

`kafka_controllers` must be an **odd** count ≥ 3; `kafka_brokers` ≥ 3. Fix inventory and re-run:

```bash
./run.sh --tags 01_validate_vars
```

### Validate fails: `kafka_node_id` missing / overlap / not unique

Every host needs a stable unique integer ID in the allowed ranges. Controllers and brokers must not share the same inventory host. After a failed partial format, do not renumber IDs — rebuild data dirs intentionally or restore the original IDs.

### Validate rejects `zookeeper` or `kafka_lbs`

Scheme B does not use ZooKeeper or an L4 VIP in front of the Kafka protocol. Remove those groups from inventory.

### Kafka archive download timeout on controller

Set a reachable mirror:

```yaml
kafka_download_mirror_url: "https://mirror.example.com/kafka/kafka_2.13-4.3.1.tgz"
```

Or place the tarball manually at `$CLUSTER_WORKSPACE_ROOT/.cache/atlas-kafka/` (filename must match `kafka_{scala}-{version}.tgz`), then re-run `--tags 200_kafka_node`.

### Controllers up, brokers fail registration

Confirm controller port `9093` is open from brokers to every controller voter, and that `controller.quorum.voters` addresses match reachable `ansible_host` values. Re-run:

```bash
./run.sh --tags 201_kafka_controllers
./run.sh --tags 202_kafka_brokers
```

### `meta.properties` / cluster.id / node.id mismatch

Usually caused by wiping some nodes, changing `kafka_node_id`, or mixing cluster IDs across hosts. Lab rebuild: stop Kafka, clear `kafka_data_dir` on **all** nodes, clear workspace `cluster.id` marker if you intend a new cluster, then re-run controllers → brokers.

### Verify: expected N brokers, got fewer IDs

Brokers may still be joining. Increase `kafka_verify_retries` / `kafka_verify_delay`, check `journalctl -u kafka` on missing brokers, then:

```bash
./run.sh --tags 203_kafka_verify
```

### Clients cannot produce (timeout / metadata)

Use a **bootstrap list of brokers** on `kafka_broker_port`. Do not point clients at controllers only. Do not put a blind L4 VIP in front of all brokers unless you understand sticky routing and metadata advertised listeners.

### Unsupported `kafka_security_mode`

Lab v1 allows `plaintext` only. SASL/TLS modes are not implemented yet — keep credentials out of git when you extend security later.

### SSH host key changed after VM recreate

```bash
ssh-keygen -f ~/.ssh/known_hosts -R '192.168.1.161'
```

## Testing / CI

Local checks (same gates as GitHub Actions):

```bash
./tests/run_ci.sh
# or piecemeal:
python3 -m unittest discover -s tests -v
ansible-playbook --syntax-check -i inventory-example.yml playbooks/kafka_cluster.yaml
ansible-lint --profile min
```

CI workflow: `.github/workflows/ci.yml` (unit tests, syntax-check, ansible-lint `min`, publish hygiene greps).

Optional stricter lint locally: `ansible-lint --profile basic` (many legacy style findings; not a merge gate yet).

## Project structure

```
atlas-kafka/
├── playbooks/
│   └── kafka_cluster.yaml
├── run.sh
├── ansible.cfg
├── requirements.yml
├── inventory-example.yml
├── group_vars/
│   └── all/
│       ├── atlas-kafka.yml
│       └── atlas-kafka.secrets.yml
├── examples/
│   ├── hosts.example.yml       # alias of inventory layout
│   └── secrets.example.yml     # EXTRA_VARS_FILE template (do not commit filled copy)
├── host_vars/
│   └── example.yml
├── roles/
│   ├── 00_ensure_workspace/
│   ├── 01_validate_vars/
│   ├── 200_kafka_node/
│   ├── 201_kafka_controllers/
│   ├── 202_kafka_brokers/
│   └── 203_kafka_verify/
├── tests/
│   ├── run_ci.sh
│   └── test_kafka_layout.py
├── .github/workflows/ci.yml
├── LICENSE
└── SECURITY.md
```

## Integrations

External orchestrators can call the same playbook with their own inventory and group/host vars. Set inventory groups (or override `kafka_controller_hosts` / `kafka_broker_hosts`) and knobs from `group_vars/all/atlas-kafka.yml` as needed (version, ports, download mirrors, heap).

Org-specific lab overlays (domains, mirrors, CA, package-repo prefixes) belong in the orchestrator inventory — not in this repository’s defaults.

Suggested tag sequence for split runs: validate → node → controllers → brokers → verify.

## Security

See [`SECURITY.md`](SECURITY.md) for reporting, secret-handling, and the pre-publish git history note. Do not commit real credentials, vault files, or live inventories (`inventory.yml` / local `hosts` are gitignored).

## Contributing

1. Keep `playbooks/kafka_cluster.yaml` as the only supported playbook entry.  
2. Document new variables in `group_vars/all/atlas-kafka.yml` and role `defaults/`.  
3. Extend `tests/test_kafka_layout.py` for layout/contract changes.  
4. Update this README when behaviour or tags change.

## Support

Open an issue in the repository for bugs and questions.
