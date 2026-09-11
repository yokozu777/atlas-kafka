# Security

## Reporting

If you discover a security issue in this repository, please open a private report with the maintainers (do not file a public issue with exploit details or credentials).

## Secrets in this repo

Do **not** commit:

- real Kafka / TLS / SASL credentials
- SSH private keys
- deprecated monolithic `secrets.yml` / `*.vault` with live values
- live inventory with production hosts (`inventory.yml`, local `hosts`)

Prefer `group_vars/all/atlas-kafka.secrets.yml` with Ansible Vault (trackable; do **not** gitignore). Standalone operators may also use `EXTRA_VARS_FILE` / `examples/secrets.example.yml` **outside** the tree. A monolithic `secrets.yml` is deprecated and must not hold live credentials in git.


Lab default security mode is `plaintext` (no AUTH). When you add credentials for non-lab deployments, keep them out of git.

Tracked examples intentionally use inert placeholders only (see `group_vars/all/atlas-kafka.yml` and `examples/secrets.example.yml`).

## Git history note (pre-publish)

Older commits (removed from the current tree) embedded a Gitea HTTPS credential in a deleted Terraform snippet:

`00_tf_kafka_nodes_ansible/files/main.tf` — `git::https://root:…@gitea.mxhash.com/…`

(Introduced in commit `622cd52`, removed from the tree in `530d480`; the blob remains reachable in git history.)

Product sources (`roles/`, `playbooks/`, `group_vars/`, examples, inventory example, runner) must stay free of org hostnames and lab credentials. Lab overlays belong in the orchestrator inventory, not in this repository’s defaults.

Before making this repository public:

1. **Rotate / revoke** that Gitea account password (and any tokens that reused it).
2. Rewrite history (`git filter-repo` / BFG) to purge the blob, or publish from a fresh orphan branch that contains only the cleaned tree.
3. Assume the credential is compromised for as long as those commits remain reachable on any remote.
