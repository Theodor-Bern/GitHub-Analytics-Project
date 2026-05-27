# DE-II Project — Course 1TD076

A distributed pipeline over Apache Pulsar that ingests recent GitHub repositories,
enriches each one with commit counts and test/CI signals, and answers four
research questions:

- **Q1** Top programming languages by project count
- **Q2** Most frequently updated projects (by commit count)
- **Q3** Top languages following test-driven development
- **Q4** Top languages following TDD + DevOps (tests + CI)

## Architecture

```
GitHub API ──► Producer ──► Pulsar broker ──┬─► Language aggregator   (Q1)
                                            ├─► Commits enricher  ──► Commit aggregator (Q2)
                                            └─► Test enricher     ──► Test aggregator   (Q3)
                                                                  └─► CI aggregator     (Q4)
```

Five VMs are provisioned on UPPMAX OpenStack:

| Role | Containers | Purpose |
|---|---|---|
| **Controller VM** | de2-controller | Orchestrates provisioning and deployment of the 4 worker VMs |
| Broker | Apache Pulsar | Message bus for the pipeline |
| Producer | producer | Fetches repos from GitHub Search API |
| Consumer | commits-enricher, test-enricher | Enrich repos with commit counts and test/CI signals |
| Aggregator | language-, commit-, test-, ci-aggregator | Produce Q1–Q4 answers |

The controller VM is created manually once. It then provisions the 4 worker VMs
automatically via OpenStack's Nova API.

## Prerequisites

On your local machine:
- An SSH client
- A web browser to access UPPMAX Horizon and GitHub

You will need:
- An OpenStack project on UPPMAX with quota for 5 `ssc.medium` VMs
- A GitHub account (1 personal access token is enough for a small demo)

## Setup

### 1. Generate an SSH keypair (on your laptop)

```bash
ssh-keygen -t rsa -b 4096 -f ~/.ssh/de2-key -N ""
chmod 600 ~/.ssh/de2-key
```

This creates two files in `~/.ssh/`:
- `de2-key`     — private key (keep secret, used on your laptop and on the controller VM)
- `de2-key.pub` — public key (uploaded to OpenStack in the next step)

### 2. Upload the public key to OpenStack

In Horizon:
1. **Compute → Key Pairs → Import Public Key**
2. Paste the contents of `~/.ssh/de2-key.pub`
3. Name it `de2-key` (or any name — remember it for `.env` later)

### 3. Download your OpenStack RC file

In Horizon:
1. **API Access → Download OpenStack RC File (v3)**
2. Save it as `~/openrc.sh` on your laptop

### 4. Create at least one GitHub Personal Access Token

On github.com:
1. **Settings → Developer settings → Personal access tokens → Tokens (classic)**
2. **Generate new token** with scopes: `public_repo`, `read:org`
3. Save it on your laptop as `~/.github_tokens`, one token per line:

   ```
   GITHUB_TOKEN_1=ghp_xxxxxxxxxxxx
   ```

   For higher throughput, add up to four more tokens (preferably from different
   GitHub accounts — tokens from the same account share the same 5000 req/h budget):

   ```
   GITHUB_TOKEN_1=ghp_xxxxxxxxxxxx
   GITHUB_TOKEN_2=ghp_xxxxxxxxxxxx   # optional
   GITHUB_TOKEN_3=ghp_xxxxxxxxxxxx   # optional
   GITHUB_TOKEN_4=ghp_xxxxxxxxxxxx   # optional
   GITHUB_TOKEN_5=ghp_xxxxxxxxxxxx   # optional
   ```

4. Lock down permissions:
   ```bash
   chmod 600 ~/.github_tokens
   ```

### 5. Create the controller VM

The controller VM is created manually in Horizon. Its cloud-init script
installs Docker, pulls the controller image, and clones this repo automatically.

1. Get the cloud-init yaml. Open this URL in your browser, select all, copy:

   ```
   https://raw.githubusercontent.com/Theodor-Bern/DE-II-Project/main/controller/cloud-init/controller.yaml
   ```

2. In Horizon: **Compute → Instances → Launch Instance**
   - **Source:** Ubuntu 22.04
   - **Flavor:** `ssc.small` (controller does no heavy compute)
   - **Networks:** UPPMAX private network
   - **Key Pair:** select `de2-key` (uploaded in step 2)
   - **Configuration → Customization Script:** paste the cloud-init yaml from step 1
   - **Launch Instance**

3. Wait until the instance is **Active** and has a private IP. Note the IP.

4. Cloud-init then runs in the background for ~5 minutes (Docker install,
   image pull, git clone). Don't SSH in yet.

### 6. SSH into the controller VM

```bash
ssh -i ~/.ssh/de2-key ubuntu@<controller-ip>
```

Wait until cloud-init is done before continuing:

```bash
ls /home/ubuntu/.cloud-init-done   # must exist
ls /home/ubuntu/de2                # repo must be cloned
docker images                      # de2-controller image must be listed
```

If `.cloud-init-done` is missing, wait a few more minutes and re-check.

### 7. Copy your credentials to the controller VM

From a **second terminal on your laptop** (keep the SSH session open):

```bash
scp -i ~/.ssh/de2-key ~/openrc.sh      ubuntu@<controller-ip>:~/openrc.sh
scp -i ~/.ssh/de2-key ~/.github_tokens ubuntu@<controller-ip>:~/.github_tokens
scp -i ~/.ssh/de2-key ~/.ssh/de2-key   ubuntu@<controller-ip>:~/.ssh/de2-key
```

The third line copies your **private key** to the controller — it needs it to
SSH onward to the 4 worker VMs.

Back in your SSH session on the controller VM, lock permissions:
```bash
chmod 600 ~/.ssh/de2-key ~/.github_tokens
```

### 8. Configure paths

On the controller VM:

```bash
cd ~/de2
cp .env.example .env
nano .env
```

Fill in (paths are on the **controller VM**, not your laptop):

```
SSH_KEY_PATH=/home/ubuntu/.ssh/de2-key
OPENSTACK_KEY_NAME=de2-key
OPENRC_PATH=/home/ubuntu/openrc.sh
GITHUB_TOKENS_PATH=/home/ubuntu/.github_tokens
DAYS_BACK=3
```

`DAYS_BACK=3` runs a quick demo (~30 minutes). The default 365 takes hours
with one GitHub token.

## Run

On the controller VM:

```bash
./run.sh deploy
```

What happens:
1. You're prompted for your OpenStack password (the same one you log into
   Horizon with).
2. **Phase 1** (~2 min): broker VM is provisioned, its IP captured.
3. **Phase 2** (~3 min): producer, consumer, aggregator VMs are provisioned
   in parallel with the broker IP injected into their cloud-init.
4. **Wait for cloud-init** (~5 min): each VM installs Docker and pulls its
   service image.
5. **Distribute**: SCPs compose files and your tokens to each VM.
6. **Start services**: broker first, then aggregators + consumers, then
   producer.

Total: ~10 min. When you see `[deploy] Cluster up`, the pipeline is running.
`deploy.sh` exits — it does not block until the producer is done.

## Collect results

Wait 10–20 min so the pipeline has time to process some data, then on the
controller VM:

```bash
./run.sh collect
```

This snapshots everything under `~/de2/results/snapshot-<timestamp>/`:

| File | Contents |
|---|---|
| `results.json` | Merged Q1–Q4 answers |
| `results_q1.json` … `results_q4.json` | Per-question raw answers |
| `*.log` | Per-aggregator container logs |
| `SUMMARY.txt` | Latest top-N from each aggregator's log |
| `figures/*.pdf` | Bar charts for Q1–Q4 |

You can run `./run.sh collect` repeatedly — each call writes a new snapshot
with the latest numbers.

## Get results onto your laptop

From your laptop:

```bash
scp -i ~/.ssh/de2-key -r ubuntu@<controller-ip>:~/de2/results/ ./
```

## Tear down

The 5 VMs persist until you delete them manually:

1. Horizon → **Compute → Instances**
2. Select all five (controller, broker-vm-*, producer-vm-*, consumer-vm-*,
   aggregator-vm-*)
3. **Actions → Delete Instances**

## Configuration reference

All overridable in `.env`:

| Variable | Default | Purpose |
|---|---|---|
| `SSH_KEY_PATH` | — (required) | Private SSH key for the worker VMs |
| `OPENSTACK_KEY_NAME` | — (required) | Name of the keypair in Horizon |
| `OPENRC_PATH` | — (required) | OpenStack RC file |
| `GITHUB_TOKENS_PATH` | — (required) | Tokens file (≥1 token) |
| `DAYS_BACK` | `7` | Days of GitHub history to scan |

## Troubleshooting

**"SSH key has wrong permissions"** → `chmod 600 ~/.ssh/de2-key` on both your
laptop and the controller VM.

**`.cloud-init-done` never appears** → Check cloud-init logs on the VM:
`sudo cat /var/log/cloud-init-output.log`. Most common issue: the Docker
apt repo was temporarily unreachable. Re-launch the VM.

**"Broker never came up"** → check UPPMAX quota; ssc.medium VMs sometimes
take several minutes to boot. Delete the worker VMs in Horizon and run
`./run.sh deploy` again.

**"GitHub rate limit exceeded"** → wait for the reset window (the token pool
handles this automatically). For a faster demo, reduce `DAYS_BACK` in `.env`.

**"Inventory not found"** → `start_instances.py` failed before writing the
inventory file. Check the deploy log for OpenStack API errors (most likely
wrong password or quota exceeded).

**Want to reset?** Delete the worker VMs in Horizon and `rm -rf state/`
before running `./run.sh deploy` again. The controller VM can be reused.
